from flask import Blueprint, request, jsonify, current_app
from .services import handle_file_upload
from .insert_service import process_and_insert_data  # DB 삽입 로직이 있는 모듈
from .analysis_services import process_all_analysis
import os  # os 모듈 가져오기

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
        merged_file_path = os.path.join(current_app.root_path, 'merged', 'merged_data.xlsx')

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


@main_bp.route('/profit_view', methods=['POST'])
def profit_view():
    try:
        html_file_path = './analysis_html/연도별_매출_판관비_순이익.html'
        if os.path.exists(html_file_path):
            with open(html_file_path, 'r', encoding='utf-8') as file:
                html_content = file.read()
            return html_content  # HTML 파일 내용 반환
        else:
            return "HTML file not found", 404
    except Exception as e:
        print("Error in /profit_view:", str(e))
        return f"Error: {str(e)}", 500


@main_bp.route('/graph', methods=['POST'])
def graph_view():
    try:
        # JSON 데이터에서 year 값 가져오기
        data = request.json
        print(f"[DEBUG] Received data: {data}")
        year = data.get('year')

        if not year:
            print("[ERROR] Year not provided in request.")
            return jsonify({'error': 'Year not provided'}), 400

        # HTML 파일 경로 리스트
        if year == "all":
            file_paths = [
                './analysis_html/연도별_재무상태표.html',
                './analysis_html/연도별_카테고리별_판매량.html',
                './analysis_html/연도별_나이대별_매출.html',
                './analysis_html/연도별_성별_매출.html',
                './analysis_html/전체_판매량_VIP.html',
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

        # 각 파일 내용 읽기
        results = []
        for path in file_paths:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as file:
                    content = file.read()
                    results.append({'path': path, 'content': content})

                    # 로그에 HTML 콘텐츠 요약 출력
                    print(f"[INFO] File found: {path}, Content Preview: {content[:100]}...")  # 앞 100자만 출력
            else:
                print(f"[WARNING] File not found: {path}")
                results.append({'path': path, 'content': f"File not found: {path}"})

        print(f"[DEBUG] Returning results count: {len(results)}")

        # HTML 파일 직접 확인
        with open('./analysis_html/연도별_지역별_판매량.html', 'r', encoding='utf-8') as file:
            content = file.read()
        print(content)

        return jsonify(results), 200

    except Exception as e:
        print(f"[EXCEPTION] Exception occurred: {str(e)}")
        return jsonify({'error': str(e)}), 500
