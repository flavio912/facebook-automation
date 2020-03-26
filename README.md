# Dropbox -> Facebook Ad Account uploader

## Installation guide

Requirements: Python 3, pip installed.

First 3 steps are from <https://packaging.python.org/guides/installing-using-pip-and-virtual-environments/>

### 1. Install virtualenv

On macOS and Linux:

```sh
python3 -m pip install --user virtualenv
```

On Windows:

```sh
py -m pip install --user virtualenv
```

### 2. Create virtualenv

On macOS and Linux:

```sh
python3 -m venv env
```

On Windows:

```sh
py -m venv env
```

### 3. Activate virtualenv

On macOS and Linux:

```sh
source env/bin/activate
```

On Windows:

```sh
.\env\Scripts\activate
```

### 4. Install dependencies

```sh
pip install -r requirements.txt
```

### 5. Install database

```sh
cd uploader_ui && manage.py migrate
```

### 6. Create superuser (To use admin panel - optional)

```sh
cd uploader_ui && manage.py createsuperuser
```

### 7. Run Unit tests

```sh
python -m unittest uploader_app
```

### 8. Run integration test (will use current configuration)

```sh
cd uploader_ui && manage.py integration_tests
```

### 8. Run development server

```sh
cd uploader_ui && manage.py runserver 8000
```

8000 here is the port number
You can also specify IP in format IP:PORT

### 9. Launch files downloading&uploading

```sh
cd uploader_ui && manage.py load
```

## Configuration

The following table provides the list of the environment variable names and their meaning

| name | description |
|---|---|
| DROPBOX_TOKEN | the dropbox API access token |
| FB_GA_APPKEY | Lucky Day Facebook App key |
| FB_GA_APPID | Lucky Day Facebook App ID |
| FB_GA_TOKEN | Lucky Day Facebook App user token |
| FB_ACT_ID | FB marketing account ID, dev act_659750741197329 |
| GA_TEMP_DIR | Directory to download files to while transferring |
| GA_ROOT | Dropbox root folder to monitor |
