import boto3
import requests

from dotenv import load_dotenv
from os import getenv
from datetime import datetime
from boto3.dynamodb.conditions import Attr
from discord_webhook import DiscordWebhook

load_dotenv()

webhook = DiscordWebhook(url=getenv('HOOK_URL'))
current_date = datetime.today().strftime('%Y-%m-%d')

class Show:
    def __init__(self, event_name, date, location, slug):
        self.event_name = event_name
        self.date = date
        self.location = location
        self.slug = slug

    def __str__(self):
        return f'{self.event_name}'

    def __repr__(self):
        return str(self)

class Corps:
    def __init__(self, name, score, division, rank):
        self.name = name
        self.score = score
        self.division = division
        self.rank = rank

    def __str__(self):
        return f'{self.rank} - {self.name} - {self.score}\n'

    def __repr__(self):
        return str(self)

def lambda_handler(event, context):

    show_slugs = []
    show_res = []
    all_ordered_placements = []
    all_show_info = []
    field_embeds = []
    
    dynamo_db = boto3.resource("dynamodb", getenv('AWS_REGION'))
    dynamo_table = dynamo_db.Table(getenv('DYNAMO_TABLE'))

    entry = dynamo_table.scan(
        FilterExpression=Attr('ShowDate').lte(current_date) & Attr('ShowRead').eq('False')
    )
    
    if len(entry['Items']) == 0:
        return
        
    for item in entry['Items']:
        show_slugs.append(item['ShowSlug'])

    for idx, slug in enumerate(show_slugs):
        res = requests.get(f'{getenv("SCORE_URL")}{slug}')
        
        if res.status_code == 400:
            continue
    
        if res.status_code == 200 and len(res.json()) != 0:
            show_res.append(res.json())

    if len(show_res) == 0:
        return
    
    for idx, specific_show in enumerate(show_res):

        divisons = []
        scores = []
        
        for show_details in specific_show:
            if show_details['divisionName'] not in divisons:
                divisons.append(show_details['divisionName'])
            scores.append(Corps(show_details['groupName'], show_details['totalScore'], show_details['divisionName'], show_details['rank']))

        ordered_placements = {key: [] for key in divisons}

        for key in ordered_placements:
            for corps in scores:
                if corps.division == key:
                    ordered_placements[key].append(corps)

            sort_scores = sorted(ordered_placements[key], key=lambda corps: corps.score, reverse=True)
            ordered_placements[key] = sort_scores
        
        all_show_info.append(Show(specific_show[0]['competition']['eventName'], specific_show[0]['competition']['date'], specific_show[0]['competition']['location'], specific_show[0]['competition']['slug']))
        all_ordered_placements.append(ordered_placements)

    for idx, show_placements in enumerate(all_ordered_placements):
        embed = []

        embed.append(
            {
                'name': 'Date',
                'value': all_show_info[idx].date.split('T')[0],
                'inline?': False
            }
        )

        embed.append(
            {
                'name': 'Location',
                'value': all_show_info[idx].location,
                'inline?': False
            }
        )

        for key, val in reversed(show_placements.items()):
            tmp = {
                'name': None,
                'value': None,
                'inline?': False,
            }

            tmp['name'] = key
            tmp['value'] = str(val).strip('[').strip(']').replace(',','')

            embed.append(tmp)

        embed.append(
            {
                'name': 'Recap',
                'value': f'https://www.dci.org/scores/recap/{all_show_info[idx].slug}'
            }
        )

        msg_embed = {
            'title': all_show_info[idx].event_name,
            'url': f'https://www.dci.org/scores/final-scores/{all_show_info[idx].slug}',
            'fields': embed,
            'image': {
                'url': f'https://production.assets.dci.org/6383b1af002bc235950f5eb9_Cf1DVvPNj8pv8jCdNANPn2Law7v3mKGl.jpeg'
            }
        }

        field_embeds.append(msg_embed)

    for show_embed in field_embeds:
        webhook.add_embed(show_embed)
        

    webhook.execute(remove_embeds=True)

    # update table
    for show in all_show_info:
        dynamo_table.update_item(
            Key={
                'ShowSlug': show.slug,
            },
            UpdateExpression='SET ShowRead = :val1',
            ExpressionAttributeValues={
                ':val1': 'True'
            }
        )
        
    return {
        'statusCode': 200,
        'body': json.dumps('Posted Scores')
    }

# for local testing purposes
if __name__ == "__main__":
    event = {
        "Items": [
            {
                "ShowName": "Drums Across the Desert",
                "ShowDate": "2023-07-03",
                "ShowSlug": "2023-drums-across-the-desert",
                "ShowRead": "False"
            },
            {
                "ShowName": "Rotary Music Festival",
                "ShowDate": "2023-07-03",
                "ShowSlug": "2023-rotary-music-festival",
                "ShowRead": "False"
            }    
        ]
    }
    lambda_handler(None, None)