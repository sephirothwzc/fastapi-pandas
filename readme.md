# 开发手册

### 编码规范

- 文件全小写下划线分词命名

###  安装手册

```shell
# 虚拟环境
$ python3 -m venv fastapienv
# 激活
$ source jitenv/bin/activate
# 在虚拟环境中安装 FastAPI
$ pip3 install fastapi
# 安装 Uvicorn 服务器
$ pip3 install "uvicorn[standard]"
# 安装 Mysql ORM
$ pip3 install 'tortoise-orm[asyncmy]'
# 依赖包及其版本信息
$ pip3 freeze > requirements.txt
# 生成依赖
$ pip3 install -r requirements.txt
```
