"""兼容入口：实际应用装配在 ``app/main.py``。"""

import uvicorn

from app.main import app

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
