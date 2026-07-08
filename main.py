# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""
SmartAgent - 智能 AI Agent 框架
基于 LangChain ReAct 架构

用法:
    python main.py             命令行模式
    python main.py --web       网页可视化模式
    python main.py --web --port 9090   自定义端口
"""

import sys
import os
import warnings

sys.path.insert(0, os.path.dirname(__file__))

# 屏蔽第三方库的 DeprecationWarning 刷屏 (anyio/starlette)
warnings.filterwarnings("ignore", category=DeprecationWarning, module="anyio")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="starlette")

if __name__ == "__main__":
    if "--web" in sys.argv:
        from src.ui.web_server import start

        # 解析端口参数
        port = 8080
        if "--port" in sys.argv:
            idx = sys.argv.index("--port")
            if idx + 1 < len(sys.argv):
                port = int(sys.argv[idx + 1])

        host = "127.0.0.1"
        if "--host" in sys.argv:
            idx = sys.argv.index("--host")
            if idx + 1 < len(sys.argv):
                host = sys.argv[idx + 1]

        start(host=host, port=port)
    else:
        from src.ui.cli import main
        main()
