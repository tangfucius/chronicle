from wtforms import Form, BooleanField, TextAreaField, TextField, PasswordField, DateField, IntegerField, SelectMultipleField, validators
#import wtforms.ext.dateutil.fields.DateField as DateField

class RecordForm(Form):
    #app = SelectMultipleField('App')
    #event = SelectMultipleField('Event (Required)', [validators.Required()])
    sd = DateField('Start date', format='%Y/%m/%d')
    dur = IntegerField('Duration (days)', default=1)
    comment = TextAreaField('Comment', [validators.optional(), validators.length(max=500)])
    url = TextField('Link', [validators.optional(), validators.URL()])