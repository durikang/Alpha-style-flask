from flask import Blueprint, request, jsonify, current_app, send_file
from .services import handle_file_upload
from .insert_service import process_and_insert_data  # DB 삽입 로직이 있는 모듈
from .analysis_services import process_all_analysis
from .table_data import generate_json_from_excel
import os  # os 모듈 가져오기
import zipfile
import io  # io 모듈을 import
import json


# 블루프린트 생성
main_bp = Blueprint('main', __name__)


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


@main_bp.route('/insert', methods=['POST'])
def insert_data():
    try:
        # 병합된 파일 경로
        merged_file_path = '../merged/merged_data.xlsx'
        # 병합된 파일 경로를 절대 경로로 설정
        merged_file_path = os.path.join('merged', 'merged_data.xlsx')

        # DB 삽입 로직 호출
        success, message = process_and_insert_data(merged_file_path, 'c##finalProject/1234@localhost:1521/xe')
        if success:
            return jsonify({'message': message}), 200
        elif message == "No valid data to insert.":
            return jsonify({'message': message}), 204
        else:
            return jsonify({'error': message}), 500

    except Exception as e:
        print("Error in /insert:", str(e))
        return jsonify({'error': str(e)}), 500


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
                './analysis_html/연도별_재무상태표.html',
                './analysis_html/연도별_카테고리별_판매량.html',
                './analysis_html/연도별_나이대별_매출.html',
                './analysis_html/연도별_성별_매출.html',
                './analysis_html/연도별_VIP_유저.html',
                './analysis_html/연도별_지역별_판매량.html'
            ]
        else:
            file_paths = [
                f'./analysis_html/{year}/{year}_재무상태표.html',
                f'./analysis_html/{year}/{year}_카테고리별_판매량.html',
                f'./analysis_html/{year}/{year}_나이대별_매출.html',
                f'./analysis_html/{year}/{year}_성별_매출.html',
                f'./analysis_html/{year}/{year}_VIP_유저.html',
                f'./analysis_html/{year}/{year}_지역별_판매량.html'
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
        year = data.get('year')

        # 엑셀 데이터 로드
        excel_data = load_excel_data()
        if not excel_data:
            return jsonify({'error': 'Failed to load data'}), 500

        # 연도별 데이터 필터링
        filtered_data = {
            "cost": [row for row in excel_data['cost'] if str(row['년도']) == year] if year != 'all' else excel_data['cost'],
            "net_profit": [row for row in excel_data['net_profit'] if str(row['년도']) == year] if year != 'all' else excel_data['net_profit'],
            "sale": [row for row in excel_data['sale'] if str(row['년도']) == year] if year != 'all' else excel_data['sale'],
            "category_sales": [row for row in excel_data['category_sales'] if str(row['년도']) == year] if year != 'all' else excel_data['category_sales'],
        }

        # Dynamic Analysis 결과 로드
        dynamic_output_path = "./analysis/dynamic_analysis.json"
        if os.path.exists(dynamic_output_path):
            with open(dynamic_output_path, 'r', encoding='utf-8') as f:
                dynamic_analysis_results = json.load(f)
        else:
            dynamic_analysis_results = []

        # 연도 필터 적용 (연도가 "all"인 경우 전체 반환)
        filtered_dynamic_analysis = [
            result for result in dynamic_analysis_results if f"{year}년" in result
        ] if year != "all" else dynamic_analysis_results

        # Dynamic Analysis 결과 추가
        filtered_data['dynamic_analysis'] = filtered_dynamic_analysis

        return jsonify(filtered_data), 200

    except Exception as e:
        print(f"[ERROR] Exception in /table-data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@main_bp.route('/download', methods=['POST'])
def download_files():
    try:
        # JSON 데이터에서 year 값 가져오기
        data = request.form  # Form 데이터로 전달
        year = data.get('year', 'all')  # year 값이 없으면 'all'로 설정

        # PNG 파일 경로 리스트 생성
        if year == "all":
            png_paths = [
                './analysis_png/연도별_재무상태표.png',
                './analysis_png/연도별_카테고리별_판매량.png',
                './analysis_png/연도별_나이대별_매출.png',
                './analysis_png/연도별_성별_매출.png',
                './analysis_png/전체_판매량_VIP.png',
                './analysis_png/연도별_지역별_판매량.png',
                './analysis/cost.xlsx',
                './analysis/net_profit.xlsx',
                './analysis/sale.xlsx',
                './analysis/나이대별_판매량.xlsx',
                './analysis/성별별_판매량.xlsx',
                './analysis/연도별_카테고리별_판매량.xlsx'
            ]
        else:
            png_paths = [
                f'./analysis_png/{year}/{year}_재무상태표.png',
                f'./analysis_png/{year}/{year}_카테고리별_판매량.png',
                f'./analysis_png/{year}/{year}_나이대별_매출.png',
                f'./analysis_png/{year}/{year}_성별_매출.png',
                f'./analysis_png/{year}/{year}_VIP_유저.png',
                f'./analysis_png/{year}/{year}_지역별_판매량.png',
                f'./analysis/{year}_VIP_유저.xlsx',
                f'./analysis/{year}_카테고리별_판매량.xlsx'
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