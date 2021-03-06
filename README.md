# crunchbase

## Video

https://www.loom.com/share/c48cd8ae1ac24dbab7c5a19b6e4a2b7c

## Installation

1. Make sure Python 3.8 or higher and git are installed.

    Windows:

    https://www.python.org/downloads/windows/

    If the installer asks to add Python to the path, check yes.

    https://git-scm.com/download/win

    MacOS:

    Open Terminal. Paste the following commands and press enter.

    ```
    ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
    echo 'export PATH="/usr/local/opt/python/libexec/bin:$PATH"' >> ~/.profile
    brew install python
    ```

    Linux:

    Open a terminal window. Paste the following commands and press enter.

    ```
    sudo apt install -y python3
    sudo apt install -y python3-pip
    sudo apt install -y git
    ```

2. Open a terminal/command prompt window. Run the following command.

    ```
    git clone (repository url)
    ```

3. Run the following commands in the same terminal/command prompt window you just opened. Depending on your system you may need run `pip` instead of `pip3`.

    ```
    cd (repository name)
    pip3 install -r requirements.txt
    ```

## Instructions

1. Save your .har as `user-data/credentials/www.crunchbase.com.har` as shown in the video. Perform the search from this url: https://www.crunchbase.com/search/organizations/field/organizations/location_identifiers/united-kingdom
2. Optionally, put your proxy list into `user-data/proxies.csv`. The header must contain `url,port,username,password`. The other lines follow that format. See `user-data/proxies.sample.csv` for an example.
3. Run `python3 main.py`. Depending on your system you may need run `python main.py` instead.
4. The output will be in `user-data/output/output.csv` and `user-data/database.sqlite`.

## Command line parameters

- `--refresh`: refresh results that are already in the database. Add companies that were founded recently. This reduces the number of searches you need to do.

## Options

`user-data/options.ini` accepts the following options:

- `runRepeatedly`: 1 means run repeatedly every x hours. 0 means run only once. Default: 1.
- `hoursBetweenRuns`: How many hours to wait between runs. Default: 168.
- `searchResultLimit`: Stop once get this many search results for a given line in input.csv. Default: 0, which means no limit.
- `resumeSearch`: 0 means only run if haven't run in `hoursBetweenRuns` hours. 1 means to run regardless of when completed last time. Default: 1.