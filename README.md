# Auto-Steer-Spark

## Requirements

### Packages

- sqlite3
    - Statistics extension (we provide a download script: `sqlean-extensions/download.sh`)
    - 这个extension目前是linux-x64版本的，如果电脑是其他操作系统请自己找到对应的包安装
- python3 (at least version 3.10)

### Python3 requirements

- Install python requirements using the file `pip3 install -r requirements.txt` 

## Run Auto-Steer

### Configuration
config file is `config.cfg`. refrences below

| key               | info                                           |
| ----------------- | ---------------------------------------------- |
| BENCHMARK         | the benchmark you want to use `job` or `tpcds` |
| THRIFT_SERVER_URL | jdbc server url                                |
| THRIFT_PORT       | jdbc server port                               |
| THRIFT_USERNAME   | your username                                  |
| EXPLAIN_THREADS   | how many thread to explain query plans         |
| REPEATS           | how many times to run a single query           |

### 运行SparkOptimizer
- run taining mode(example)
```commandline
python main.py
```

- run test mode(example)
```commandline
python main.py --test
```

- 使用`--debug`参数可以调整日志输出等级为debug

## 每次运行前后需要关注的文件

 `results/*.sqlite` ： `train`过程中产生的数据集会保存在指定的数据库中，两次指定同一个数据库会导致两次的数据合并在一个数据库里，一般需要每次指定不一样的，或者将上一次的删除再指定相同的数据库

## 其他重要文件

`data/knobs.txt`：每一行一个可以开关的knob，可以用于手动指定要探索的knobs
