# Python 3.9 이미지를 베이스로 사용합니다. 버전은 필요에 따라 조정하세요.
FROM python:3.9

# 작업 디렉토리를 설정합니다.
WORKDIR /app

# 의존성 파일들을 복사합니다.
COPY requirements.txt .

# 의존성을 설치합니다.
RUN pip install --no-cache-dir -r requirements.txt

# 현재 디렉토리의 모든 파일을 컨테이너의 작업 디렉토리로 복사합니다.
COPY . .

# 애플리케이션이 사용할 포트를 지정합니다.
EXPOSE 10001

# 컨테이너가 시작될 때 실행할 명령어를 정의합니다.
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]

