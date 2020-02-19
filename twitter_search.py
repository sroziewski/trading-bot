# import twitter
#
# api = twitter.Api()
# statuses = api.GetUserTimeline(screen_name = "binance")
# print([s.text for s in statuses])
#
# api = twitter.Api(consumer_key='twitter consumer key',
#                   consumer_secret='twitter consumer secret',
#                   access_token_key='the_key_given',
#                   access_token_secret='the_key_secret')


# import urllib.request
# with urllib.request.urlopen('https://twitter.com/binance') as response:
#    html = response.read()
#    i = 1

import shutil
import tempfile
import urllib.request

# with urllib.request.urlopen() as response:
#     with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
#         shutil.copyfileobj(response, tmp_file)


req = urllib.request.Request('https://twitter.com/binance')
with urllib.request.urlopen(req) as response:
   the_page = response.read()
   if 'Community Coin Vote Round'.lower() in str(the_page).lower():
        i = 1

# with open(tmp_file.name) as html:
#     html.__str__()
#     "Community Coin Vote Round"
#     pass
#