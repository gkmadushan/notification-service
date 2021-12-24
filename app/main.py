import pika
import json
import os
import sys
import requests
from dotenv import load_dotenv
import time
import base64
from smtplib import SMTP, SMTPException
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

load_dotenv()

sender = os.getenv('EMAIL_SENDER')
smtp_host = os.getenv('EMAIL_HOST')
smtp_port = os.getenv('EMAIL_PORT')
smtp_username = os.getenv('EMAIL_USERNAME')
smtp_password = os.getenv('EMAIL_PASSWORD')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')


def send_email(to, subject, msg, html=False):
    receivers = to
    message = MIMEMultipart('alternative')
    message['Subject'] = subject
    message['From'] = sender
    message['To'] = ", ".join(to)
    message.attach(MIMEText(msg, 'plain'))
    if html != False:
        message.attach(MIMEText(html, 'html'))

    try:
        smtpObj = SMTP(smtp_host, smtp_port)
        smtpObj.starttls()
        smtpObj.ehlo()
        smtpObj.login(smtp_username, smtp_password)
        smtpObj.sendmail(sender, receivers, message.as_string())
        smtpObj.quit()
        return True
    except SMTPException as e:
        print(sys.exc_info())
        return e


def main(ch, method, properties, body):
    try:
        details = json.loads(body)
        print(details)
        send_email(details['to'], details['subject'], msg=details['message'], html=details['message'])

    except Exception as e:
        print('EXCEPTION'+str(sys.exc_info()), file=sys.stderr)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def subscribe():
    while(True):
        time.sleep(2.4)
        print('Consumer Starting...')
        connection = pika.BlockingConnection(pika.ConnectionParameters(os.environ['RABBITMQ_HOST']))
        channel = connection.channel()
        channel.queue_declare(queue='notifications', durable=True)
        channel.basic_qos(prefetch_count=5)
        channel.basic_consume(queue='notifications', auto_ack=False, on_message_callback=main)

        try:
            channel.start_consuming()
        except pika.exceptions.ConnectionClosedByBroker as e:
            print(e)
            continue
        except pika.exceptions.AMQPChannelError as err:
            print(err)
            break
        except pika.exceptions.AMQPConnectionError as e2:
            print(e2)
            continue
        except Exception as ex:
            break


print('Notification service')
subscribe()
