import os
from app import create_app
import sys

if __name__ == '__main__':
	if len(sys.argv)>1:
		config = sys.argv[1]
	else:
		config = None

	app = create_app(config)
	app.run(host='0.0.0.0', debug=True)