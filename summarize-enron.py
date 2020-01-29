import sys
import logging
import matplotlib.pyplot as plt
from datetime import datetime
import pandas as pd
from collections import Counter

from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()


logger = logging.getLogger(__name__)


msg = "%(asctime)s %(name)s %(levelname)s: %(message)s"


def init_logger(level=logging.INFO, log_file=None):
    logging.root.handlers = []
    logger.setLevel(level)

    sh = logging.StreamHandler()
    sh.setLevel(level)
    sh.setFormatter(logging.Formatter(msg))
    logger.addHandler(sh)

    if log_file is not None:
        fh = logging.FileHandler(log_file)
        fh.setLevel(level)
        fh.setFormatter(logging.Formatter(msg))
        logger.addHandler(fh)


headers = ['timestamp', 'message_identifier', 'sender', 'recipients', 'topic', 'mode']

list_emails = []

def transform_data(row_df):
    ts = datetime.fromtimestamp(int(row_df['timestamp'])/1000)
    recipients_list = list(set([r.strip() for r in row_df['recipients'].split('|')]))
    sender = row_df['sender'].strip()

    email_obj = dict(
        timestamps=ts,
        message_identifier=row_df['message_identifier'],
        sender=sender,
        recipients_list=recipients_list,
    )
    list_emails.append(email_obj)


def read_and_clean_data(csv_path):
    logger.info('Read data from csv file')
    df = pd.read_csv(csv_path)
    df.columns = headers
    logger.info('Delete the unused columns')
    del df['topic']
    del df['mode']
    logger.info('Fill null values of sender and recipients columns')
    df['sender'] = df['sender'].fillna('unknown_sender')
    df['recipients'] = df['recipients'].fillna('unknown_recipients')
    logger.info('Transform data...')
    for _, row in df.iterrows():
        transform_data(row)


def get_result():
    logger.info('Generate the result...')
    all_recipients = []
    all_senders = []

    for i in list_emails:
        all_recipients.append(i['recipients_list'])
        all_senders.append(i['sender'])

    flattened_all_recipients = [item for sublist in all_recipients for item in sublist]
    recip_counts = Counter(flattened_all_recipients)
    sender_counts = Counter(all_senders)
    all_person = set(flattened_all_recipients + all_senders)

    name_counts_list = [{'person': n,
                         'sent': sender_counts[n],
                         'received': recip_counts[n]} for n in all_person]
    counts_df = pd.DataFrame(name_counts_list)
    return counts_df.sort_values('sent', ascending=False)


def gen_person_activity(person_name):
    emails_activity = []
    for e in list_emails:
        sent = 0
        received = 0
        sender_name = None

        if e['sender'] == person_name or person_name in e['recipients_list']:
            if e['sender'] == person_name:
                sent = 1
            if person_name in e['recipients_list']:
                received = 1
                sender_name = e['sender']

            emails_activity.append({
                'time_stamp': e['timestamps'],
                'sent': sent,
                'received': received,
                'sender_name': sender_name,
                'person_name': person_name
            })
    return pd.DataFrame(emails_activity).set_index('time_stamp')


def visualize_sent(top_persons):
    """ A PNG image visualizing the number of emails sent over time by some of the most prolific senders """
    plt.figure()
    for _, p in enumerate(top_persons):
        p_activity = gen_person_activity(person_name=p)
        sent_agg = p_activity['sent'].groupby(pd.Grouper(freq='M')).sum()
        plt.plot(sent_agg.index, sent_agg.values, label=p)
        plt.title('Number of emails sent every month')
        plt.grid(True)
        plt.xlabel("Unix Time (month)")
        plt.ylabel("Number of emails")
        plt.legend(loc=2, borderaxespad=0.,frameon=False)
        plt.savefig('number_email_sent.png', format='png', dpi=200)


def visualize_received(top_persons):
    """A visualization that shows, for the same people, the number of unique people/email addresses who contacted them
       over the same time period. """
    plt.figure()
    for _, p in enumerate(top_persons):
        p_activity = gen_person_activity(person_name=p)
        received_from_unique_agg = p_activity['sender_name'].groupby(pd.Grouper(freq='M')).nunique()
        plt.plot(received_from_unique_agg,label=p)
        plt.title('Number of unique senders every month')
        plt.grid(True)
        plt.xlabel('Unix Time (month)')
        plt.ylabel('Number of unique senders')
        plt.legend(loc=2, borderaxespad=0., frameon=False)
        plt.savefig('number_unique_people.png', format='png',dpi=200)


if __name__ == '__main__':
    init_logger(logging.INFO)
    raw_csv_path = sys.argv[1]
    read_and_clean_data(raw_csv_path)
    df_result = get_result()
    df_result.to_csv('result.csv', index=False)
    top_5_sender = list(df_result['person'].iloc[0:5])
    visualize_sent(top_5_sender)
    visualize_received(top_5_sender)
