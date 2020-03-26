FROM python:3.8.0

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m pip install --user virtualenv && python3 -m virtualenv --python=/usr/bin/python3 $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install dependencies:
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

# Run the application:
COPY uploader_ui .
RUN python manage.py migrate
CMD ["python", "manage.py", "load"]
