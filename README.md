## 해당 프로젝트에 필요한 모듈 설치하기

1. `requirements.txt`에 적힌 모듈을 설치하기 위해서는 아래 명령어를 입력해야 합니다:
    ```bash
    pip install -r requirements.txt
    ```

2. 만약 가상환경에 접속하지 않은 경우, 가상환경에 접속한 후 위 명령어를 실행합니다.

3. 새로운 모듈을 추가할 경우, 아래 명령어를 입력하여 `requirements.txt` 파일을 업데이트합니다:
    ```bash
    pip freeze > requirements.txt
    ```

4. 설치한 모듈을 테스트하려면 아래 명령어를 실행합니다:
    ```bash
    pip install -r requirements.txt
    ```
