FROM python:3.6

EXPOSE 5000

WORKDIR /broker2

COPY . /broker2

RUN pip install -r requirements.txt

CMD python broker.py