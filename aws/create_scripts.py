# copy files from airflow/scripts, adding in AWS lambda compatibility to top level scripts
import re
import shutil
import os
 
# path to source directory
src_dir = '../airflow/scripts/'
 
# path to destination directory
dest_dir = './scripts'
 
# getting all the files in the source directory
files = os.listdir(src_dir)
 
if os.path.exists(dest_dir):
    shutil.rmtree(dest_dir)
shutil.copytree(src_dir, dest_dir)

scripts = [
    './scripts/extract_company_info.py',
    './scripts/extract_posts_data.py',
    './scripts/extract_subreddit_info.py',
    './scripts/update_posts_data.py',
    './scripts/subreddit_agg.py',
    './scripts/subreddit_symbol_agg.py'
]

for file in scripts:

    with open(file, 'r') as f:
        data = f.readlines()
    add_handler = False
    for i, line in enumerate(data):
        if 'import' in line:
            pass
        elif 'import' not in line and not add_handler:
            data[i] = 'import logging\ndef handler(event, context):\n    logging.getLogger().setLevel(logging.INFO)\n'
            add_handler = True
        else:
            if 'args' in line:
                line = re.sub(r'args = get_args\(\)', '', line)
                line = re.sub(r'(args.)([a-z_]*)', r"event['\2']", line)
            data[i] = "    " + line

    if file == './scripts/update_posts_data.py':
        data.insert(0, "import os\nos.environ['TRANSFORMERS_CACHE'] = '/tmp/'\nfrom subprocess import call\n")
        data.append("\n    call('rm -rf /tmp/*', shell=True)\n")

    data.append('\n    return "End of file. Exiting script..."')

    with open(file, 'w') as f:
        f.writelines(data)


with open('./scripts/reddit_to_postgres.py', "r+") as f:
    content = f.read()
    content = re.sub(r'logging\.getLogger\(\)\.setLevel\(logging\.INFO\)', '', content, flags = re.M)
    f.seek(0)
    f.write(content)
    f.truncate()


