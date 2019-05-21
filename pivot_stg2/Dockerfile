FROM python:3.6-slim

WORKDIR /usr/app

ADD ./requirements.txt ./
RUN pip install -r requirements.txt --proxy http://proxy.com:8080
ADD ./ ./

CMD ["python", "app.py"]
