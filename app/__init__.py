from flask import Flask
from flask_cors import CORS
import os

def create_app():
    app = Flask(__name__)
    CORS(app)

    # 업로드 및 병합 데이터 저장 폴더 설정
    UPLOAD_FOLDER = './uploads'
    MERGED_FOLDER = './merged'
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    os.makedirs(MERGED_FOLDER, exist_ok=True)

    app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
    app.config['MERGED_FOLDER'] = MERGED_FOLDER

    # 블루프린트 등록
    from .routes import main_bp
    app.register_blueprint(main_bp)

    return app
