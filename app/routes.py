from flask import Blueprint, request, jsonify, send_file,Response, stream_with_context,current_app
from .services import handle_file_upload
from .insert_service import process_and_insert_data  # DB 삽입 로직이 있는 모듈
###from .analysis_services import process_all_analysis
from .table_data import generate_json_from_excel
from app.test import process_all_analysis
import os  # os 모듈 가져오기
import zipfile
import io  # io 모듈을 import
import json
import pandas as pd
from .createExcelService import generate_financial_excel_stream
from .analysis_service import analyze_product_sales

import time
import threading

# 블루프린트 생성
main_bp = Blueprint('main', __name__)

# 작업 진행 상태를 저장하는 전역 변수
tasks_status = {"cancel": False}

# SSE 상태를 관리하기 위한 전역 변수
sse_messages = []


@main_bp.route('/upload', methods=['POST'])
def upload_files():
    try:
        UPLOAD_FOLDER = current_app.config['UPLOAD_FOLDER']
        MERGED_FOLDER = current_app.config['MERGED_FOLDER']

        files = request.files.getlist('files')
        if not files:
            return jsonify({'status': 'error', 'message': '업로드할 파일이 없습니다.'}), 400

        # sse_messages, tasks_status 초기화
        sse_messages.clear()
        tasks_status["cancel"] = False

        os.makedirs(UPLOAD_FOLDER, exist_ok=True)
        os.makedirs(MERGED_FOLDER, exist_ok=True)

        file_paths = []
        for file in files:
            file_path = os.path.join(UPLOAD_FOLDER, file.filename)
            file.save(file_path)
            file_paths.append(file_path)

        # 백그라운드 스레드로 병합
        thread = threading.Thread(
            target=background_merge,
            args=(file_paths, MERGED_FOLDER)
        )
        thread.start()

        # 한글 메시지로 반환
        return jsonify({
            'status': 'started',
            'message': '파일 업로드가 완료되었습니다. 백그라운드에서 병합을 진행합니다...'
        }), 200

    except Exception as e:
        return jsonify({'status': 'error', 'message': f'오류 발생: {str(e)}'}), 500


def background_merge(file_paths, merged_folder):
    """
    백그라운드 스레드: 직접 merged_folder 사용
    """
    try:
        dataframes = []
        total = len(file_paths)

        for index, file_path in enumerate(file_paths, start=1):
            if tasks_status["cancel"]:
                sse_messages.append("업로드가 취소되었습니다.")
                return

            # 파일 로딩
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.endswith('.xlsx'):
                df = pd.read_excel(file_path)
            else:
                sse_messages.append(f"지원되지 않는 파일 형식: {os.path.basename(file_path)}")
                return

            dataframes.append(df)
            # SSE 메시지 → 한글로 표현
            sse_messages.append(f"파일 {index}/{total} 업로드 완료: {os.path.basename(file_path)}")

            time.sleep(1)  # 시연용 딜레이

        if not dataframes:
            sse_messages.append("병합할 유효한 파일이 없습니다.")
            return

        # 병합
        sse_messages.append("데이터 병합 중...")
        merged_df = pd.concat(dataframes, ignore_index=True)

        output_file_path = os.path.join(merged_folder, 'merged_data.xlsx')
        merged_df.to_excel(output_file_path, index=False)

        sse_messages.append("파일 병합 완료.")

    except Exception as e:
        sse_messages.append(f"에러 발생: {str(e)}")



@main_bp.route('/progress')
def progress_stream():
    """
    SSE로 업로드/병합 진행 상황을 스트리밍 (한국어 메시지).
    """
    def generate():
        last_index = 0
        while True:
            if last_index < len(sse_messages):
                for message in sse_messages[last_index:]:
                    yield f"data: {message}\n\n"
                last_index = len(sse_messages)
            time.sleep(1)

    return Response(stream_with_context(generate()), mimetype="text/event-stream")


# DB삽입 라우터
@main_bp.route('/insert', methods=['GET', 'POST'])
def insert_data():
    """데이터 삽입 작업 진행"""
    if request.method == 'POST':
        # POST 요청으로 데이터 삽입 시작
        return jsonify({'message': '데이터 삽입 작업이 시작되었습니다.'}), 200

    # GET 요청: 작업 진행 상황 스트리밍
    tasks_status["cancel"] = False

    def generate_progress():
        try:
            print("[DEBUG] /insert 요청 수신")

            # 병합된 파일 경로
            merged_file_path = os.path.join('./merged', 'merged_data.xlsx')
            db_connection_string = 'c##finalProject/1234@localhost:1521/xe'

            yield f"data: {json.dumps({'progress': 5, 'message': '병합된 파일 읽는 중: {merged_file_path}'})}\n\n"

            # 1. 파일 읽기
            try:
                data = pd.read_excel(merged_file_path)
                yield f"data: {json.dumps({'progress': 15, 'message': f'병합된 파일에서 {len(data)}개 행을 읽었습니다.'})}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'progress': 0, 'message': f'데이터 읽기 실패: {str(e)}'})}\n\n"
                return

            # 2. 데이터 전처리
            try:
                yield f"data: {json.dumps({'progress': 20, 'message': '데이터 전처리 중...'})}\n\n"
                # 필수 컬럼의 결측값 채우기
                required_columns = ['유저번호', '년도', '월', '일', '공급가액']
                data[required_columns] = data[required_columns].fillna(0)
                yield f"data: {json.dumps({'progress': 30, 'message': '필수 컬럼의 결측값을 0으로 채웠습니다.'})}\n\n"

                # 날짜 유효성 검증 및 변환
                data['order_date'] = pd.to_datetime(
                    data[['년도', '월', '일']].astype(int).astype(str).agg(''.join, axis=1),
                    format='%Y%m%d',
                    errors='coerce'
                )
                invalid_dates = data['order_date'].isnull().sum()
                data = data.dropna(subset=['order_date'])
                yield f"data: {json.dumps({'progress': 50, 'message': f'유효하지 않은 날짜 {invalid_dates}개 제거. 남은 데이터: {len(data)}개.'})}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'progress': 0, 'message': f'데이터 전처리 실패: {str(e)}'})}\n\n"
                return

            # 3. 데이터베이스 삽입
            try:
                yield f"data: {json.dumps({'progress': 60, 'message': 'Oracle DB 연결 중...'})}\n\n"
                success, message = process_and_insert_data(merged_file_path, db_connection_string)
                if success:
                    yield f"data: {json.dumps({'progress': 90, 'message': '데이터베이스에 성공적으로 삽입되었습니다.'})}\n\n"
                else:
                    yield f"data: {json.dumps({'progress': 0, 'message': f'삽입 실패: {message}'})}\n\n"
                    return
            except Exception as e:
                yield f"data: {json.dumps({'progress': 0, 'message': f'데이터베이스 삽입 중 오류 발생: {str(e)}'})}\n\n"
                return

            # 4. 완료
            yield f"data: {json.dumps({'progress': 100, 'message': '작업이 성공적으로 완료되었습니다!'})}\n\n"

        except GeneratorExit:
            print("[DEBUG] 스트리밍 연결 종료")
        except Exception as e:
            print(f"[ERROR] /insert 작업 중 에러 발생: {e}")
            yield f"data: {json.dumps({'progress': 0, 'message': f'오류 발생: {str(e)}'})}\n\n"

    return Response(generate_progress(), content_type='text/event-stream')


# 분서하기 라우터
@main_bp.route('/analysis', methods=['GET', 'POST'])
def create_graph():
    """
    분석 작업 라우트:
      - POST: "분석 작업이 시작되었습니다." JSON 응답
      - GET : SSE로 진행률 스트리밍 (test.py 수정)
    """
    if request.method == 'POST':
        return jsonify({"message": "분석 작업이 시작되었습니다."}), 200

    # GET → SSE
    tasks_status["cancel"] = False

    def generate_analysis():
        try:
            print("[DEBUG] /analysis SSE 요청 수신")

            # test.py 의 process_all_analysis 를 단계별로 호출/진행률 전송
            # => process_all_analysis()가 한 번에 끝나므로, 중간 단계별로 yield 필요
            # 아래는 예시로 "가짜" 단계별 yield를 삽입.
            # 실제론 test.py 내부에 별도 함수를 두어 step마다 yield 하도록 재구성할 수도 있음.

            yield sse_data({"progress": 10, "message": "분석 준비 중..."})
            time.sleep(1)

            success, message = process_all_analysis()
            if success:
                # 중간 단계들(20%, 50%, 70%...)을 더 넣으려면 test.py 내부를 수정해서
                # process_all_analysis()가 step마다 콜백/yield 하도록 해야 함
                yield sse_data({"progress": 100, "message": message})
            else:
                yield sse_data({"progress": 0, "message": f"분석 실패: {message}"})

        except GeneratorExit:
            print("[DEBUG] /analysis 스트리밍 연결 종료")
        except Exception as e:
            print(f"[ERROR] /analysis 작업 중 에러: {e}")
            yield sse_data({"progress": 0, "message": f"오류 발생: {str(e)}"})

    return Response(stream_with_context(generate_analysis()), content_type='text/event-stream')



@main_bp.route('/graph', methods=['POST'])
def graph_view():
    try:
        # JSON 데이터에서 year 값 가져오기
        data = request.json
        print(f"[DEBUG] Received data: {data}")
        year = data.get('year')

        if not year:
            return jsonify({'error': 'Year not provided'}), 400

        # HTML 파일 경로 리스트
        if year == "all":
            file_paths = [
                './analysis/html/연도별_재무상태표.html',
                './analysis/html/연도별_카테고리별_판매량.html',
                './analysis/html/연도별_나이대별_매출.html',
                './analysis/html/연도별_성별_매출.html',
                './analysis/html/연도별_VIP_유저.html',
                './analysis/html/연도별_지역별_판매량.html'
            ]
        else:
            file_paths = [
                f'./analysis/html/{year}/{year}_재무상태표.html',
                f'./analysis/html/{year}/{year}_카테고리별_판매량.html',
                f'./analysis/html/{year}/{year}_나이대별_매출.html',
                f'./analysis/html/{year}/{year}_성별_매출.html',
                f'./analysis/html/{year}/{year}_VIP_유저.html',
                f'./analysis/html/{year}/{year}_지역별_판매량.html'
            ]

        print(f"[DEBUG] File paths: {file_paths}")

        # 각 HTML 파일 내용 읽기
        html_results = []
        for path in file_paths:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as file:
                    html_results.append({'path': path, 'content': file.read()})
            else:
                html_results.append({'path': path, 'content': f"File not found: {path}"})

        print(f"[DEBUG] Returning results count: {len(html_results)}")

        # Excel 데이터에서 JSON 생성 (summary와 category_sales 데이터)
        table_data = generate_json_from_excel(year if year != "all" else None)
        print(json.dumps(table_data, indent=4, ensure_ascii=False))

        # 결과 반환
        return jsonify({"html_results": html_results, "table_data": table_data}), 200

    except Exception as e:
        print(f"[EXCEPTION] Exception occurred: {str(e)}")
        return jsonify({'error': str(e)}), 500

@main_bp.route('/table-data', methods=['POST'])
def get_table_data():
    try:
        # 요청에서 연도 가져오기
        data = request.json
        year = data.get('year') or 'all'

        # Excel 데이터를 로드하여 JSON 생성
        excel_data = generate_json_from_excel(year)
        if not excel_data:
            return jsonify({'error': 'Failed to load data'}), 500

        response_data = {
            "summary": excel_data.get("summary", []),
            "category_sales": excel_data.get("category_sales", []),
            "gender_sales": excel_data.get("gender_sales", []),
            "age_group_sales": excel_data.get("age_group_sales", []),
            "vip_sales": excel_data.get("vip_sales", []),
            "area_sales": excel_data.get("area_sales", []),
        }

        return jsonify(response_data), 200

    except Exception as e:
        print(f"[ERROR] Exception in /table-data: {str(e)}")
        return jsonify({'error': str(e)}), 500
'''
@main_bp.route('/download', methods=['POST'])
def download_files():
    try:
        # JSON 데이터에서 year 값 가져오기
        data = request.form  # Form 데이터로 전달
        year = data.get('year', 'all')  # year 값이 없으면 'all'로 설정

        # PNG 파일 경로 리스트 생성
        if year == "all":
            png_paths = [
                './analysis/png/연도별_재무상태표.png',
                './analysis/png/연도별_카테고리별_판매량.png',
                './analysis/png/연도별_나이대별_매출.png',
                './analysis/png/연도별_성별_매출.png',
                './analysis/png/연도별_VIP_유저.png',
                './analysis/png/연도별_지역별_판매량.png',
                './analysis/xlsx/cost.xlsx',
                './analysis/xlsx/net_profit.xlsx',
                './analysis/xlsx/sale.xlsx',
                './analysis/xlsx/나이대별_판매량.xlsx',
                './analysis/xlsx/성별별_판매량.xlsx',
                './analysis/xlsx/연도별_카테고리별_판매량.xlsx'
                './analysis/xlsx/연도별_VIP_유저.xlsx'
                './analysis/xlsx/연도별_지역별_판매량.xlsx'

            ]
        else:
            png_paths = [
                f'./analysis/png/{year}/{year}_재무상태표.png',
                f'./analysis/png/{year}/{year}_카테고리별_판매량.png',
                f'./analysis/png/{year}/{year}_나이대별_매출.png',
                f'./analysis/png/{year}/{year}_성별_매출.png',
                f'./analysis/png/{year}/{year}_VIP_유저.png',
                f'./analysis/png/{year}/{year}_지역별_판매량.png',
                f'./analysis/xlsx/{year}/{year}_VIP_유저.xlsx'
                f'./analysis/xlsx/{year}/{year}_나이대별_판매량.xlsx'
                f'./analysis/xlsx/{year}/{year}_성별_매출.xlsx'
                f'./analysis/xlsx/{year}/{year}_지역별_판매량.xlsx'
                f'./analysis/xlsx/{year}/{year}_카테고리별_판매량.xlsx'
            ]

        # ZIP 파일 생성
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for path in png_paths:
                if os.path.exists(path):
                    zip_file.write(path, arcname=os.path.basename(path))
                else:
                    print(f"[WARNING] File not found: {path}")

        zip_buffer.seek(0)

        # ZIP 파일 전송
        return send_file(
            zip_buffer,
            mimetype='application/zip',
            as_attachment=True,
            download_name=f"{year}_files.zip"
        )
    except Exception as e:
        print(f"[EXCEPTION] Exception occurred: {str(e)}")
        return jsonify({'error': str(e)}), 500
'''
@main_bp.route('/download', methods=['POST'])
def download_files():
    try:
        # POST 요청에서 year 값 가져오기
        year = request.form.get('year', 'all')
        print(f"[DEBUG] 전달된 'year' 값: {year}")

        # 고정 파일 경로
        fixed_paths = [
            './analysis/png/연도별_재무상태표.png',
            './analysis/png/연도별_카테고리별_판매량.png',
            './analysis/png/연도별_나이대별_매출.png',
            './analysis/png/연도별_성별_매출.png',
            './analysis/png/전체_판매량_VIP.png',
            './analysis/png/연도별_지역별_판매량.png',
            './analysis/xlsx/cost.xlsx',
            './analysis/xlsx/net_profit.xlsx',
            './analysis/xlsx/sale.xlsx',
            './analysis/xlsx/나이대별_판매량.xlsx',
            './analysis/xlsx/성별별_판매량.xlsx',
            './analysis/xlsx/연도별_카테고리별_판매량.xlsx',
            './analysis/xlsx/연도별_VIP_유저.xlsx',
            './analysis/xlsx/연도별_지역별_판매량.xlsx',
            './analysis/xlsx/엑셀데이터종합본.xlsx'
        ]

        # 연도별 파일 경로 리스트
        file_paths = {}
        for y in range(2020, 2025):
            file_paths[y] = [
                f'./analysis/png/{y}/{y}_재무상태표.png',
                f'./analysis/png/{y}/{y}_카테고리별_판매량.png',
                f'./analysis/png/{y}/{y}_나이대별_매출.png',
                f'./analysis/png/{y}/{y}_성별_매출.png',
                f'./analysis/png/{y}/{y}_VIP_유저.png',
                f'./analysis/png/{y}/{y}_지역별_판매량.png',
                f'./analysis/xlsx/{y}/{y}_VIP_유저.xlsx',
                f'./analysis/xlsx/{y}/{y}_나이대별_판매량.xlsx',
                f'./analysis/xlsx/{y}/{y}_성별_매출.xlsx',
                f'./analysis/xlsx/{y}/{y}_지역별_판매량.xlsx',
                f'./analysis/xlsx/{y}/{y}_카테고리별_판매량.xlsx',
            ]

        # ZIP 파일 생성 (메모리)
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # 고정 파일 PNG 번호 초기화
            png_counter = 1

            # 고정 파일 추가
            for path in fixed_paths:
                if os.path.exists(path):
                    file_extension = os.path.splitext(path)[1].lower()
                    if file_extension == ".png":
                        # PNG 파일 이름을 순서대로 변경
                        arcname = f"{png_counter}.png"
                        png_counter += 1
                    else:
                        # 원래 파일 이름 유지
                        arcname = os.path.basename(path)
                    zip_file.write(path, arcname=arcname)
                else:
                    print(f"[WARNING] File not found: {path}")

            # 연도별 파일 추가 (PNG 파일 이름을 순서대로 변경)
            for year, paths in file_paths.items():
                year_png_counter = 1  # 연도별 PNG 파일 번호 초기화
                for path in paths:
                    if os.path.exists(path):
                        # 파일 확장자 확인
                        file_extension = os.path.splitext(path)[1].lower()
                        if file_extension == ".png":
                            # PNG 파일 이름을 순서대로 변경
                            arcname = f"{year}/{year_png_counter}.png"
                            year_png_counter += 1
                        else:
                            # 원래 파일 이름 유지
                            arcname = f"{year}/{os.path.basename(path)}"
                        zip_file.write(path, arcname=arcname)
                    else:
                        print(f"[WARNING] File not found: {path}")

        # 메모리 버퍼의 시작 위치로 이동
        zip_buffer.seek(0)

        # ZIP 파일 전송
        return send_file(
            zip_buffer,
            mimetype='application/zip',
            as_attachment=True,
            download_name="all_files.zip"
        )
    except Exception as e:
        print(f"[EXCEPTION] Exception occurred: {str(e)}")
        return jsonify({'error': str(e)}), 500

@main_bp.route('/financial/download', methods=['GET'])
def download_financial_excel():
    """
    금융 기록 데이터를 엑셀로 다운로드 (실시간 진행 상황 제공)
    """
    try:
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')
        transaction_type = request.args.get('transactionType')

        def generate():
            try:
                total_chunks = 0
                for chunk in generate_financial_excel_stream(start_date, end_date, transaction_type):
                    total_chunks += 1
                    yield chunk
                    # 진행 상황 전송
                    yield f"event:progress\ndata:{total_chunks * 10}%\n\n".encode('utf-8')
                yield f"event:complete\ndata:Download complete.\n\n".encode('utf-8')
            except Exception as e:
                yield f"event:error\ndata:{str(e)}\n\n".encode('utf-8')

        headers = {
            'Content-Disposition': 'attachment; filename=financial_records.xlsx',
            'Content-Type': 'application/octet-stream'
        }

        return Response(stream_with_context(generate()), headers=headers)

    except Exception as e:
        return Response(json.dumps({'error': str(e)}), status=500, mimetype='application/json')

@main_bp.route('/cancel', methods=['POST'])
def cancel_task():
    """
    업로드 취소
    """
    tasks_status["cancel"] = True
    sse_messages.append("사용자에 의해 업로드가 취소되었습니다.")
    return jsonify({'message': '업로드가 취소되었습니다.'}), 200


def sse_data(obj):
    """
    SSE 포맷으로 데이터를 한 번에 보내기 위한 헬퍼.
    obj: dict{"progress":..., "message":...} 식
    """
    return f"data: {json.dumps(obj, ensure_ascii=False)}\n\n"


@main_bp.route('/salesAnalysis', methods=['GET'])
def sales_analysis():
    """
    GET 파라미터
      - startDate (YYYY-MM-DD)
      - endDate   (YYYY-MM-DD)
      - chartType (pie, bar, line ...)
    예: /salesAnalysis?startDate=2025-01-01&endDate=2025-01-31&chartType=bar

    응답(JSON):
      { "success": true/false,
        "plotly_data": [...],    # Plotly 그래프의 data
        "plotly_layout": {...}   # Plotly 그래프의 layout
        "error": "에러메시지"    # 실패 시 에러 메시지
      }
    """
    try:
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')
        chart_type = request.args.get('chartType', 'bar')  # 기본값 bar

        if not start_date or not end_date:
            return jsonify({"success": False, "error": "시작일/종료일이 누락되었습니다."}), 400

        # DB 커넥션 문자열(예시)
        db_connection_string = 'c##finalProject/1234@localhost:1521/xe'

        success, result = analyze_product_sales(start_date, end_date, chart_type, db_connection_string)
        if success:
            return jsonify({
                "success": True,
                "plotly_data": result["data"],
                "plotly_layout": result["layout"]
            }), 200
        else:
            return jsonify({
                "success": False,
                "error": result["error"]
            }), 500

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500