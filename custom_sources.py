"Custom source importer"
import os
import static.filesystem as fs
import static.settings as settings
import sources
from datetime import datetime
import sql
from sql import Post, File, URL, Hash
from sqlalchemy.sql.expression import func

#datetime.now().strftime("%c %X")
settings_file = fs.find_file('settings.json')
_loaded = settings.load(settings_file)
_session = sql.session()

url_patt = r'^(?:(?!(youtu\.be|youtube\.com|amazon\.c|twitter\.c|instagram\.com)).)*$'

def strf_utc(sec):
    return datetime.fromtimestamp(sec).strftime("%y-%m-%d %H:%M:%S")

def user_source(name, alias=None, ps=False, limit=None, deep=False, check_last=None, check_utc=0):
    u = name.replace('/u/', '').replace('u/', '').strip('/')
    out = {'alias': alias or u,
            'data': {'user': u,
                    'users': u,
                    'limit': 0,
                    'scan_comments': False,
                    'scan_submissions': True,
                    'scan_limit': limit,
                    'deep_scan_comments': False,
                    'deep_scan_submissions': deep,
                    'check_last_seen_posts': check_last,
                    'check_last_seen_utc': check_utc},
  #    'filters': {}, # handled by handlers.disabled_link
    'type': 'pushshift-user-source' if ps else 'user-posts-source'}
    return out 

def resrc(u, **kwargs):
    re = sources.UserPostsSource()
    re.from_obj(user_source(u, **kwargs))
    return re

def pssrc(u, **kwargs):
    ps = sources.PushShiftUserSourceSource()
    ps.from_obj(user_source(u, **kwargs))
    return ps

def get_urls(post):
    return [x.split('?',1)[0] for x in post.get_urls()]

def userlist_file():
    lnum = 0
    with open('userlist', 'r') as f:
        for u in f:
            lnum += 1
            u = u.split('#')[0].strip()
            if not u:
                continue
            yield u

def load_userlist():
    #existing_users = [x[0] for x in _session.query(Post.author).distinct()]
    newest_utc = dict(_session.query(Post.author, func.max(Post.created_utc)).group_by(Post.author))
    source_dicts = []
    if os.path.exists('userlist'):
        lnum = 0
        with open('userlist', 'r') as f:
            for u in f:
                lnum += 1
                if lnum <= -1 or lnum > 10000:
                   continue
                u = u.split('#')[0].strip()
                if not u:
                    continue
                if u in newest_utc:
                    check_utc = newest_utc[u]
                    check_last = 50
                    check_utc = min(check_utc, datetime(2021, 3, 31).timestamp())
                    source_dicts.append(user_source(u, alias='p%03d %s'%(lnum,u), check_last=check_last, check_utc=check_utc, ps=True))
                    source_dicts.append(user_source(u, alias='r%03d %s'%(lnum,u), check_last=check_last, check_utc=check_utc, ps=False))
                elif u:
                    print(lnum, '(no posts found)', u)
                    source_dicts.append(user_source(u, alias='p%03d %s (new)'%(lnum,u), deep=True, ps=True))
                    source_dicts.append(user_source(u, alias='r%03d %s (new)'%(lnum,u), deep=True, ps=False))
        print("\n%d sources added from userlist"%len(source_dicts))
        settings.put('sources', source_dicts, save_after=False)

        #sources.load_sources(user_source('me'))

def find_duplicates():
    users = []
    if os.path.exists('userlist'):
        lnum = 0
        with open('userlist', 'r') as f:
            for u in f:
                lnum += 1
                u = u.split('#')[0].strip()
                if u:
                    if u in users:
                        print(lnum, u)
                    else:
                        users.append(u)

def sql_get_last_processed_post(username):
    return _session.query(Post).filter(Post.author==username).order_by(Post.created_utc.desc()).first()

def is_new_post(p, seen):
    if p.created_utc > max(p.created_utc for p in seen):
        return True
    if p.created_utc < min(p.created_utc for p in seen):
        return False
    for s in seen:
        if p.id != s.reddit_id:
            return False
    print("missed older post:", p.id)
    return True

if __name__ == 'x__main__':
    from sql import Post, File, URL, Hash
    # posts = list(source.get_elements())
    for u in [][1:]:
        last_seen = sql.get_last_seen_posts(u, 100)
        ps = sources.PushShiftUserSourceSource()
        ps.from_obj(user_source(u, check_last=100, ps=True))
        ps_posts = list(ps.get_elements())
        print('u: %s ps len: %s'%(u, len(ps_posts)))
        re = sources.UserPostsSource()
        re.from_obj(user_source(u, check_last=100, ps=False))
        re_posts = list(re.get_elements())
        print('u: %s re len: %s'%(u, len(re_posts)))

if __name__ == 'x__main__':
    from sql import Post, File, URL, Hash
    from sqlalchemy.orm import joinedload,contains_eager

    print('sarting sql session')
    urls = _session.query(sql.URL)
    q = _session.query(File).options(joinedload(File.urls))
    str( _session.query(File).join(URL).options(contains_eager(File.urls)))
    hashed = set(int(r.file_id) for r in _session.query(Hash.file_id).filter(Hash.full_hash != None, Hash.file_id != None))
    downloaded = set(r.id for r in _session.query(File).filter(File.downloaded == True))
    unfinished = _session.query(File).filter(File.id.in_(downloaded.difference(hashed))).all()

    print('ready')

sub_patt = r'(?:gonewild|gw)'
if __name__ == '__main__':
    import pickle, re
    from psaw import PushshiftAPI
    ps = PushshiftAPI()
    ms = sources.UpvotedSaved()
    ## %time ms_posts = list(ms)
    ## pickle.dump(ms_posts, open(datetime.now().strftime("%y.%m.%d.ms_posts.pkl"), 'wb'))
    # with open('../ms_posts.pkl', 'rb') as f:
    #     ms_posts = pickle.load(f)
    ## maybe = [p for p in new if not re.search(sub_patt, p.subreddit, re.I) and p.subreddit != 'IRLgirls' and p.over_18 and p.author not in list(userlist_file())]
    ## print('\n'.join("{} {:<20} {:<20} {:<8} {:<32} {}".format(strf_utc(p.created_utc), p.subreddit, p.author, p.id, p.title[:30], p.url) for p in del_ps.values()))
    ## print('\n'.join("{} {:<20} {:<20} {:<8} {:<32} \n   {}\n".format(strf_utc(p.created_utc), p.subreddit, p.author, p.id, p.title[:30], '\n   '.join(p.get_urls())) for p in definate))
    userlist = list(userlist_file())
    # nu = [p.author for p in ms_posts if p.author not in userlist]
    # ns = [p.subreddit for p in ms_posts if p.author not in userlist]
    # out = []
    # for u,s in zip(nu, ns):
    #     print(strf_utc(datetime.now().timestamp()), u, s)
    #     r = resrc(u, limit=100, deep=True).get_elements()
    #     for i,p in enumerate(r):
    #         if re.search(sub_patt, p.subreddit, re.I):
    #             out.append((u, i, p.subreddit))
    #             print('   ', i, p.subreddit)
    #             break
    #     else:
    #         out.append((u, -1, ''))
    #         print('   ', i, '                                NOT FOUND')


#0030083039180018 #vidble dead link
