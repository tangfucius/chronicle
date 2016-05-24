from flask import Blueprint, render_template, redirect, request, url_for, flash, session
from flask.ext.login import UserMixin, current_user, login_required, login_user, logout_user
from saml2 import (
    BINDING_HTTP_POST,
    BINDING_HTTP_REDIRECT,
    entity,
)
from saml2.client import Saml2Client
from saml2.config import Config as Saml2Config
import requests
import logging
from logging.handlers import TimedRotatingFileHandler
from .. import login_manager

auth = Blueprint('auth', __name__)

user_store = ['kf.tang@funplus.com']

#setup logging
logger = logging.getLogger()
fmt = '%(asctime)s %(levelname)s %(pathname)s %(module)s.%(funcName)s Line:%(lineno)d %(message)s'
hdlr = TimedRotatingFileHandler('/tmp/okta_callback.log', 'midnight', 1, 100)
hdlr.setFormatter(logging.Formatter(fmt))
logger.addHandler(hdlr)
logger.setLevel(logging.WARNING)

def _get_saml_client():
    acs_url = url_for('.okta_acs', _external=True)
    rv = requests.get('https://funplus.okta.com/app/exk4p9c57lLAhkXu00x7/sso/saml/metadata')
    import tempfile
    tmp = tempfile.NamedTemporaryFile()
    print rv.text
    f = open(tmp.name, 'w')
    f.write(rv.text)
    f.close()
    saml_settings = {
        'metadata': {
            'inline': [rv.text],
        },
        'service': {
            'sp': {
                'endpoints': {
                    'assertion_consumer_service': [
                        (acs_url, BINDING_HTTP_REDIRECT),
                        (acs_url, BINDING_HTTP_POST)
                    ],
                },
                'allow_unsolicited': True,
                'authn_requests_signed': False,
                'logout_requests_signed': True,
                'want_assertions_signed': True,
                'want_response_signed': False,
            },
        },
    }

    spConfig = Saml2Config()
    spConfig.load(saml_settings)
    spConfig.allow_unknown_attributes = True
    saml_client = Saml2Client(config=spConfig)
    tmp.close()
    return saml_client

class User(UserMixin):
    def __init__(self, user_id):
        user = {}
        self.id = None
        try:
            user = user_id
            self.id = unicode(user_id)
        except:
            pass

@auth.route('/saml/sso/chronicle-okta/account/acs/', methods = ['POST'])
def okta_acs():
    saml_client = _get_saml_client()
    resp = request.form['SAMLResponse']

    authn_response = saml_client.parse_authn_request_response(
        resp, entity.BINDING_HTTP_POST)
    user_fields = authn_response.get_identity()
    username = authn_response.get_subject().text
    
    if username in user_store:
    	user = User(username)
    	print user.id
    	session['saml_attributes'] = authn_response.ava
    	login_user(user)
    	url = url_for('main.compare')
    	return redirect(url)
    else:
    	return "Permission Denied", 401

@auth.route('/')
def signin():
    saml_client = _get_saml_client()
    _, info = saml_client.prepare_for_authenticate()

    redirect_url=None
    for key, value in info['headers']:
        if key == 'Location':
            redirect_url = value
            break
    print redirect_url
    return redirect(redirect_url, code=302)

@login_manager.user_loader
def load_user(user_id):
    return User(user_id)
