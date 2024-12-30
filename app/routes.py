from flask import Blueprint, request, jsonify
from .services import handle_file_upload
from .insert_service import process_and_insert_data  # DB 삽입 로직이 있는 모듈

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
        merged_file_path = './merged/merged_data.xlsx'

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
