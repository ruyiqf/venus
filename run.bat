@echo off
echo 启动定时数据任务
set current_dir= D:\matlab日数据\venus_002
pushd %current_dir%
python D:\matlab日数据\venus_002\venus\start.py
popd
