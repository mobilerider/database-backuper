export MANDRILL_APIKEY="le-mandrill-key"
export PYRAX_USERNAME="le-pyrax-username"
export PYRAX_APIKEY="le-pyrax-password"
# If the requirements are installed in a virtualenv, set here the full path
# to the Python interpreter of that virtualenv. To use the system Python,
# just set this to "python", but you would have to install the requirements
# globally (which may require using "sudo")
export PYTHON="/le/path/to/python"

$PYTHON backuper.py | $PYTHON reporter.py