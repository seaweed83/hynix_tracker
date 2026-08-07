import contextlib
import io
import logging
import os
import sys

from dulwich import porcelain

logging.getLogger('dulwich').setLevel(logging.CRITICAL)
for _n in list(logging.root.manager.loggerDict):
    if _n.startswith('dulwich'):
        logging.getLogger(_n).setLevel(logging.CRITICAL)

REPO = '/Users/chengli/Documents/trae_projects/hynix_tracker'
CRED = os.path.expanduser('~/.git-credentials')


def load_credentials():
    if not os.path.exists(CRED):
        return None, None
    line = open(CRED).read().strip().splitlines()[0]
    rest = line.split('://', 1)[1]
    userpass = rest.split('@', 1)[0]
    if ':' in userpass:
        u, p = userpass.split(':', 1)
        return u, p
    return userpass, None


def tree_path_sha(repo, tree_id, path):
    tree = repo[tree_id]
    parts = path.encode().split(b'/')
    for p in parts[:-1]:
        if p not in tree:
            return None
        tree = repo[tree[p][1]]
    return tree[parts[-1]][1] if parts[-1] in tree else None


def main():
    repo = porcelain.open_repo(REPO)
    porcelain.add(repo, paths=['yeren.db'])

    idx = repo.open_index()
    new_sha = idx[b'yeren.db'].sha
    old_sha = None
    try:
        old_sha = tree_path_sha(repo, repo[repo.head()].tree, 'yeren.db')
    except KeyError:
        pass
    if new_sha == old_sha:
        print('yeren.db 无变化，跳过 push')
        return

    author = 'chengli <chengli@localhost>'
    porcelain.commit(repo, message='chore: 每日野人哥数据同步 (dulwich)'.encode('utf-8'), author=author, committer=author)

    cfg = repo.get_config()
    url = cfg.get((b'remote', b'origin'), b'url')
    if not url:
        print('未找到 origin url'); sys.exit(1)
    user, pwd = load_credentials()
    creds = (user.encode(), (pwd or '').encode()) if user else None
    _silent = io.BytesIO()
    try:
        porcelain.push(repo, remote_location=url.decode(), refspecs=(b'refs/heads/main',),
                       credentials=creds, outstream=_silent, errstream=_silent)
    except Exception as e:
        print('push 失败(凭据不打印):', e)
        sys.exit(1)
    print('已 push yeren.db')


if __name__ == '__main__':
    main()
