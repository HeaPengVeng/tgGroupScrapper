from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetFullChatRequest
from telethon.tl.types import InputPeerEmpty
import asyncio 
import csv 
import sys, os
import time


groupCSV = 'groupsProd'
api_id = int( os.environ['TELGRAM_API_ID'] )
api_hash = os.environ['TELGRAM_API_HASH'] 
progressCSV = 'progress'


def toCSV(fileName, list):
  keys = list[0].keys()
  with open(fileName, 'w', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(list)

def fromCSV(fileName):
  with open(f'{fileName}.csv') as intput_file:
    list = [{k: v for k, v in row.items()}
        for row in csv.DictReader(intput_file, skipinitialspace=True)]
  return list

def getProgress():
  latestId = -1
  answer = input('Do you want to conitnue last scriping ? (y/n): ')
  if(answer == 'y' and os.path.isfile(f'./{progressCSV}.csv')):
    progress = fromCSV(progressCSV)
    latestId = progress[-1]['curId']
  elif os.path.isfile(f'./{progressCSV}.csv'):
    os.remove(f'./{progressCSV}.csv')

  return latestId


async def scrapUsers(botToken, groups,latestId):
    botId = botToken.split(':')[0]
    totalGroups = 0
    totalScrapped = 0
    totalFailed = 0
    totalSkipped = 0
    progress = 0
    resultDir = f'./{botId}-{time.time()}'
    try:
      client = TelegramClient(botId, api_id, api_hash)
      await client.start(bot_token=botToken)
      os.mkdir(path=resultDir)
      for group in groups:
        totalGroups += 1
        try:
          print(f"Checking {group['group_title']} with bot {botId}")
          if group['bot_id'] == botId and int(group['id']) > int(latestId):
            groupId = group['chat_id'].replace('-','')
            print(f"Scraping {group['group_title']}:{groupId} with bot {botId}")
            result = await client(GetFullChatRequest(int(groupId)))
            users = result.users
            usersList = []
            if len(users) < 1:
              totalScrapped += 1
              print(f'No users found in group')
              continue 
            for user in users:
              print(f"Found user {user.username}")
              usersList.append({
                'userId': user.id,
                'username': user.username,
                'fname': user.first_name,
                'lname': user.last_name,
                'phone': user.phone
              })
            saveFileName = f"{groupId}_{group['group_title']}"
            toCSV(f'{resultDir}/{saveFileName}', usersList)
            progress=group['id']
            totalScrapped += 1
            print(f"Done scraping {group['group_title']} with bot {botId} ON FILE NAME: {resultDir}/{saveFileName}")
          else:
            totalSkipped += 1
        except Exception as e:
          totalFailed += 1
          print(f"Failed {group['group_title']} with bot {botId}")
          exc_type, exc_obj, exc_tb = sys.exc_info()
          fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
          print(exc_type, fname, exc_tb.tb_lineno, e)
        finally:
          toCSV(progressCSV, [{"curId": progress, "time": int(time.time())}])
    except Exception as e:
      print(e)
    finally:
      print(f"Total: {totalGroups}, Scrapped: {totalScrapped}, Skipped: {totalSkipped}, Failed: {totalFailed}")

fromCSVData = fromCSV(groupCSV)
latestId = -1
botTokens = []

while True:
  userInput = input('Please input bot token (Type done if that\'s all): ')
  if userInput == 'done':
    break
  botTokens.append(userInput)

for botToken in botTokens:
 try:
    print(botToken)
    asyncio.run(scrapUsers(botToken=botToken, groups=fromCSVData,latestId=latestId))
 except Exception as e:
    print(e)
