from config import TG_TOKEN
from db import init_db
from bot_handlers import build_application


def main():
    init_db()
    app = build_application(TG_TOKEN)
    print("APP OBJECT:", app)
    if app is None:
        raise RuntimeError("build_application() вернул None")
    app.run_polling(allowed_updates=None)


if __name__ == "__main__":
    main()
