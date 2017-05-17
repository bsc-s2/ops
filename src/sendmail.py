import smtplib
from email.mime.text import MIMEText

mail_host="smtp.mail.com"
mail_user="username"
mail_pass="passwd"

def send_mail(to_list, subject, content):

    me="me<me@mail.com>"

    msg = MIMEText(content, _subtype='html', _charset='utf-8')
    msg['To'] = ";".join(to_list)
    msg['From'] = me
    msg['Subject'] = subject

    s = smtplib.SMTP()
    s.connect(mail_host)
    s.login(mail_user, mail_pass)
    s.sendmail(me, to_list, msg.as_string())
    s.close()

if __name__ == '__main__':
    send_mail(['me@mail.com'], "test",
            "<a href='http://www.baishancloud.com'>www.baishancloud.com</a>")
