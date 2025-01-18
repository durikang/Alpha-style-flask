from app import create_app

# Flask 애플리케이션 생성
app = create_app()

if __name__ == '__main__':
    # 애플리케이션 실행
    app.run(debug=True, threaded=True)
