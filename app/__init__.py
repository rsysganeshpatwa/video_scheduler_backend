from flask import Flask
from .routes import routes
from .scheduler import initialize_scheduler

def create_app():
    app = Flask(__name__)
    app.register_blueprint(routes)
    initialize_scheduler(app)
    return app
