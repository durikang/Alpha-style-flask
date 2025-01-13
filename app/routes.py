from flask import Blueprint, request, jsonify, send_file,Response
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

import time
import threading

# 블루프린트 생성
main_bp = Blueprint('main', __name__)

# 작업 진행 상태를 저장하는 전역 변수
tasks_status = {"cancel": False}

@main_bp.route('/upload', methods=['POST'])
def upload_files():
    try:
        files = request.files.getlist('files')
        if not files:
            return jsonify({'error': 'No files provided'}), 400

        # 업로드된 파일 디버깅
        print("Uploaded Files:", [file.filename for file in files])

        result = handle_file_upload(files)
        print("Handle File Upload Result:", result)  # 디버깅용 출력
        return jsonify(result), 200

    except Exception as e:
        print("Error in /upload:", str(e))  # 예외 디버깅
        return jsonify({'error': str(e)}), 500


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



@main_bp.route('/cancel', methods=['POST'])
def cancel_task():
    """작업 취소"""
    tasks_status["cancel"] = True
    print("작업 취소 요청 처리 완료")  # 디버깅 로그 추가
    return jsonify({'message': '작업이 취소되었습니다.'}), 200


@main_bp.route('/analysis', methods=['POST'])
def create_graph():
    try:
        success, message = process_all_analysis()
        if success:
            return jsonify({"message": message}), 200
        else:
            return jsonify({"error": message}), 500
    except Exception as e:
        print("Error in /analysis:", str(e))
        return jsonify({'error': str(e)}), 500

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
                './analysis/png/전체_판매량_VIP.png',
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
