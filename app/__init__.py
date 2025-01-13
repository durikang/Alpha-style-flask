from flask import Flask
from flask_cors import CORS
import os

def create_app():
    """
    Flask 앱을 생성하고 필요한 설정 및 블루프린트를 등록합니다.
    """
    app = Flask(__name__)

    # CORS 설정: 모든 경로와 도메인을 허용
    CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)

    # 업로드 및 병합 데이터 저장 폴더 설정
    UPLOAD_FOLDER = './uploads'
    MERGED_FOLDER = './merged'

    # 폴더가 없으면 생성
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    os.makedirs(MERGED_FOLDER, exist_ok=True)

    # Flask 앱 구성 설정
    app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
    app.config['MERGED_FOLDER'] = MERGED_FOLDER

    # 디버그 모드에서 로깅 추가
    if app.debug:
        import logging
        logging.basicConfig(level=logging.DEBUG)
        app.logger.info("Debug 모드 활성화")

    # 블루프린트 등록
    from .routes import main_bp
    app.register_blueprint(main_bp)

    # 시작 시 로깅
    app.logger.info("Flask 앱 초기화 완료")
    app.logger.info(f"UPLOAD_FOLDER: {UPLOAD_FOLDER}")
    app.logger.info(f"MERGED_FOLDER: {MERGED_FOLDER}")

    return app
