from __future__ import print_function
import uuid

import os
from multiprocessing import Process

import redis
import time
import pickle
from bluelens_spawning_pool import spawning_pool
from stylelens_image.images import Images

from bluelens_log import Logging


HOST_URL = 'host_url'
TAG = 'tag'
SUB_CATEGORY = 'sub_category'
PRODUCT_NAME = 'product_name'
IMAGE_URL = 'image'
PRODUCT_PRICE = 'product_price'
CURRENCY_UNIT = 'currency_unit'
PRODUCT_URL = 'product_url'
PRODUCT_NO = 'product_no'
MAIN = 'main'
NATION = 'nation'

SPAWN_MAX = 20

REDIS_OBJECT_INDEX_DONE = 'bl:object:index:done'
REDIS_PRODUCT_CLASSIFY_QUEUE = 'bl:product:classify:queue'
# REDIS_CRAWL_VERSION = 'bl:crawl:version'
# REDIS_CRAWL_VERSION_LATEST = 'latest'
REDIS_IMAGE_INDEX_QUEUE = 'bl:image:index:queue'

REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
RELEASE_MODE = os.environ['RELEASE_MODE']
DB_INDEX_HOST = os.environ['DB_INDEX_HOST']
DB_INDEX_PORT = os.environ['DB_INDEX_PORT']
DB_INDEX_NAME = os.environ['DB_INDEX_NAME']
DB_INDEX_USER = os.environ['DB_INDEX_USER']
DB_INDEX_PASSWORD = os.environ['DB_INDEX_PASSWORD']

DB_OBJECT_HOST = os.environ['DB_OBJECT_HOST']
DB_OBJECT_PORT = os.environ['DB_OBJECT_PORT']
DB_OBJECT_NAME = os.environ['DB_OBJECT_NAME']
DB_OBJECT_USER = os.environ['DB_OBJECT_USER']
DB_OBJECT_PASSWORD = os.environ['DB_OBJECT_PASSWORD']

AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

rconn = redis.StrictRedis(REDIS_SERVER, port=6379, password=REDIS_PASSWORD)
options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}
log = Logging(options, tag='bl-image-index')


def query(version_id):
  log.info('start query: ' + version_id)

  # version_id = get_latest_crawl_version()
  image_api = Images()

  q_offset = 0
  q_limit = 100

  try:
    while True:
      res = image_api.get_images(version_id=version_id,
                                 offset=q_offset, limit=q_limit)
      push_images_to_queue(res)
      if q_limit > len(res):
        break
      else:
        q_offset = q_offset + q_limit
  except Exception as e:
    log.error(str(e))

  cleanup_images(image_api, version_id)

def cleanup_images(image_api, version_id):
  try:
    res = image_api.delete_images(version_id=version_id, except_version=True)
    log.debug(res)
  except Exception as e:
    log.error(e)

# def get_latest_crawl_version():
#   value = rconn.hget(REDIS_CRAWL_VERSION, REDIS_CRAWL_VERSION_LATEST)
#   version_id = value.decode("utf-8")
#   return version_id

def push_images_to_queue(images):
  rconn.lpush(REDIS_IMAGE_INDEX_QUEUE, pickle.dumps(images))

def spawn_indexer(uuid):

  pool = spawning_pool.SpawningPool()

  project_name = 'bl-image-indexer-' + uuid
  log.debug('spawn_image-indexer: ' + project_name)

  pool.setServerUrl(REDIS_SERVER)
  pool.setServerPassword(REDIS_PASSWORD)
  pool.setApiVersion('v1')
  pool.setKind('Pod')
  pool.setMetadataName(project_name)
  pool.setMetadataNamespace(RELEASE_MODE)
  pool.addMetadataLabel('name', project_name)
  pool.addMetadataLabel('group', 'bl-image-indexer')
  pool.addMetadataLabel('SPAWN_ID', uuid)
  container = pool.createContainer()
  pool.setContainerName(container, project_name)
  pool.addContainerEnv(container, 'AWS_ACCESS_KEY', AWS_ACCESS_KEY)
  pool.addContainerEnv(container, 'AWS_SECRET_ACCESS_KEY', AWS_SECRET_ACCESS_KEY)
  pool.addContainerEnv(container, 'REDIS_SERVER', REDIS_SERVER)
  pool.addContainerEnv(container, 'REDIS_PASSWORD', REDIS_PASSWORD)
  pool.addContainerEnv(container, 'SPAWN_ID', uuid)
  pool.addContainerEnv(container, 'RELEASE_MODE', RELEASE_MODE)
  pool.addContainerEnv(container, 'DB_INDEX_HOST', DB_INDEX_HOST)
  pool.addContainerEnv(container, 'DB_INDEX_PORT', DB_INDEX_PORT)
  pool.addContainerEnv(container, 'DB_INDEX_USER', DB_INDEX_USER)
  pool.addContainerEnv(container, 'DB_INDEX_PASSWORD', DB_INDEX_PASSWORD)
  pool.addContainerEnv(container, 'DB_INDEX_NAME',  DB_INDEX_NAME)
  pool.addContainerEnv(container, 'DB_OBJECT_HOST', DB_OBJECT_HOST)
  pool.addContainerEnv(container, 'DB_OBJECT_PORT', DB_OBJECT_PORT)
  pool.addContainerEnv(container, 'DB_OBJECT_USER', DB_OBJECT_USER)
  pool.addContainerEnv(container, 'DB_OBJECT_PASSWORD', DB_OBJECT_PASSWORD)
  pool.addContainerEnv(container, 'DB_OBJECT_NAME', DB_OBJECT_NAME)
  pool.setContainerImage(container, 'bluelens/bl-image-indexer:' + RELEASE_MODE)
  pool.addContainer(container)
  pool.setRestartPolicy('Never')
  pool.spawn()

def dispatch_query_job(rconn):
  while True:
    key, value = rconn.blpop([REDIS_OBJECT_INDEX_DONE])
    query(value.decode('utf-8'))

def dispatch_indexer(rconn):

  count = 0
  while True:
    len = rconn.llen(REDIS_IMAGE_INDEX_QUEUE)
    if len > 0 and count < SPAWN_MAX:
      spawn_indexer(str(uuid.uuid4()))
      count = count + 1
    time.sleep(60 + count * 10)

if __name__ == '__main__':
  try:
    Process(target=dispatch_query_job, args=(rconn,)).start()
    Process(target=dispatch_indexer, args=(rconn,)).start()
  except Exception as e:
    log.error(str(e))
