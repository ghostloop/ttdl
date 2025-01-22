# ttdl

## Overview

This project consists of a Python script `main.py` that performs a certain task. Before running the script, ensure you have properly configured it by editing the configuration variables at the top of the script.

## Configuration

Before running the script, make sure to edit the configuration variables located at the top of the `main.py` file according to your requirements.

## Installation

Before running the script, ensure that you have the necessary dependencies installed.

python packages:

- [aiofiles](https://pypi.org/project/aiofiles/)
- [aiohttp](https://pypi.org/project/aiohttp/)
- [playwright](https://pypi.org/project/playwright/)
- [requests](https://pypi.org/project/requests/)
- [tqdm](https://pypi.org/project/tqdm/)

Use the following command to install the FireFox dependancy for playwright:

```python -m playwright install firefox```

## Usage

To run the script:

```python main.py```

Make sure you have properly configured the script by editing the configuration variables as mentioned above.

To add a user to the database:

```python main.py add <username>```

To run the script for a single user (be sure to have the user added to the database first):

```python main.py run --user <username>```

The script will create a folder in the `downloads` directory for each user, and download the videos and images for each user.

You can also run the script with the `run` command to download all users at once.

```python main.py run```
