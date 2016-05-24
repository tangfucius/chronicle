# -*- coding: utf-8 -*-
from flask import Flask
from flask.ext.bootstrap import Bootstrap
from flask.ext.mail import Mail
from flask.ext.moment import Moment
#from flask.ext.sqlalchemy import SQLAlchemy
from flask.ext.login import LoginManager
#from config import config
import os
import uuid

bootstrap = Bootstrap()
mail = Mail()
moment = Moment()
#db = SQLAlchemy()
basedir = os.path.abspath(os.path.dirname(__file__))
print basedir

login_manager = LoginManager()
#login_manager.session_protection = 'strong'
login_manager.login_view = 'auth.signin'


def create_app(config_name):
    app = Flask(__name__)
    app.secret_key = str(uuid.uuid4())
    app.config['BOOTSTRAP_SERVE_LOCAL'] = True
    if config_name=='test':
        app.config['ES_LOCALHOST'] = True
    #app.config.from_object(config[config_name])

    #config[config_name].init_app(app)

    bootstrap.init_app(app)
    #mail.init_app(app)
    #moment.init_app(app)
    #db.init_app(app)
    login_manager.init_app(app)

    """if not app.debug and not app.testing and not app.config['SSL_DISABLE']:
        from flask.ext.sslify import SSLify
        sslify = SSLify(app)"""

    from .main import main as main_blueprint
    app.register_blueprint(main_blueprint)

    from .auth import auth as auth_blueprint
    app.register_blueprint(auth_blueprint, url_prefix='/auth')

    return app