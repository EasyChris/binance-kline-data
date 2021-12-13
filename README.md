# Download binance public kline data

Download binance public kline data and auto increase 1 min kline data

下载币安历史 K 线数据，以及自动更新，默认是自动更新一分钟数据，会自动添加到 mongodb 数据库

# How to use

## Clone project

```
git clone git@github.com:EasyChris/binance-kline-data.git
```

## Install docker and docker-compose

```
# install docker by shell
curl -fsSL https://get.docker.com -o get-docker.sh
# run install docker shell
sh get-docker.sh
# install docker-compose and git
apt install docker-compose git
```

## Run mongodb docker

```
cd mongodb
mkdir data init
docker-compose up -d
```

## Edit config

open config.py and add symbol that you need

if you need add eth/usdt coin pair,in config.py file, `symbol_list` add "ETHUSDT" in symbol_list

## Start download kline

```
pip install -r requirements.txt

python data_index.py
```

# Thanks

[binance-public-data](https://github.com/binance/binance-public-data)
