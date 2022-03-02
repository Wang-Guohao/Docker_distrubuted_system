FROM python:3.6

EXPOSE 5100

WORKDIR /publisher

COPY requirements.txt /publisher
RUN pip install -r requirements.txt

COPY pub.py /publisher
CMD python pub.py
