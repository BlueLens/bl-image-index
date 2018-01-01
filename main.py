from __future__ import print_function
import uuid

import os
from multiprocessing import Process

import redis
import time
from bluelens_spawning_pool import spawning_pool
from stylelens_product.products import Products
from stylelens_product.crawls import Crawls
from stylelens_object.objects import Objects

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

REDIS_OBJECT_INDEX_DONE = 'bl:object:index:done'
REDIS_OBJECT_INDEX_QUEUE = 'bl:object:index:queue'
REDIS_IMAGE_INDEX_QUEUE = 'bl:image:index:queue'
REDIS_CRAWL_VERSION = 'bl:crawl:version'
REDIS_CRAWL_VERSION_LATEST = 'latest'

REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
RELEASE_MODE = os.environ['RELEASE_MODE']
DB_INDEX_HOST = os.environ['DB_INDEX_HOST']
DB_INDEX_PORT = os.environ['DB_INDEX_PORT']
DB_INDEX_NAME = os.environ['DB_INDEX_NAME']
DB_INDEX_USER = os.environ['DB_INDEX_USER']
DB_INDEX_PASSWORD = os.environ['DB_INDEX_PASSWORD']

MAX_PROCESS_NUM = int(os.environ['MAX_PROCESS_NUM'])

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


product_api = None
object_api = None
image_api = None

def get_latest_crawl_version(rconn):
  value = rconn.hget(REDIS_CRAWL_VERSION, REDIS_CRAWL_VERSION_LATEST)
  log.debug(value)
  try:
    version_id = value.decode("utf-8")
  except Exception as e:
    log.error(str(e))
    version_id = None
  return version_id

def cleanup_images(image_api, version_id):
  try:
    res = image_api.delete_images(version_id=version_id, except_version=True)
    log.debug(res)
  except Exception as e:
    log.error(e)

def spawn(version_id):
  spawn_id = str(uuid.uuid4())

  pool = spawning_pool.SpawningPool()

  project_name = 'bl-image-indexer-' + spawn_id
  log.debug('spawn_image-indexer: ' + project_name)

  pool.setServerUrl(REDIS_SERVER)
  pool.setServerPassword(REDIS_PASSWORD)
  pool.setApiVersion('v1')
  pool.setKind('Pod')
  pool.setMetadataName(project_name)
  pool.setMetadataNamespace(RELEASE_MODE)
  pool.addMetadataLabel('name', project_name)
  pool.addMetadataLabel('group', 'bl-image-indexer')
  pool.addMetadataLabel('SPAWN_ID', spawn_id)
  container = pool.createContainer()
  pool.setContainerName(container, project_name)
  pool.addContainerEnv(container, 'AWS_ACCESS_KEY', AWS_ACCESS_KEY)
  pool.addContainerEnv(container, 'AWS_SECRET_ACCESS_KEY', AWS_SECRET_ACCESS_KEY)
  pool.addContainerEnv(container, 'REDIS_SERVER', REDIS_SERVER)
  pool.addContainerEnv(container, 'REDIS_PASSWORD', REDIS_PASSWORD)
  pool.addContainerEnv(container, 'SPAWN_ID', spawn_id)
  pool.addContainerEnv(container, 'VERSION_ID', version_id)
  pool.addContainerEnv(container, 'MAX_PROCESS_NUM', MAX_PROCESS_NUM)
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

def prepare_objects_to_index(rconn, version_id):
  global object_api
  offset = 0
  limit = 100

  try:
    while True:
      res = object_api.get_objects(version_id=version_id,
                                   is_indexed=True,
                                   offset=offset,
                                   limit=limit)
      log.debug("Got " + str(len(res)) + ' products')
      for object in res:
        rconn.lpush(REDIS_IMAGE_INDEX_QUEUE, str(object['_id']))

      if limit > len(res):
        break
      else:
        offset = offset + limit

  except Exception as e:
    log.error(str(e))

def dispatch(rconn, version_id):
  size = rconn.llen(REDIS_IMAGE_INDEX_QUEUE)

  if size < MAX_PROCESS_NUM:
    for i in range(10):
      spawn(str(uuid.uuid4()))
    time.sleep(60*60*2)

  if size >= MAX_PROCESS_NUM and size < MAX_PROCESS_NUM*10:
    for i in range(300):
      spawn(str(uuid.uuid4()))
    time.sleep(60*60*5)

  elif size >= MAX_PROCESS_NUM*100:
    for i in range(500):
      spawn(str(uuid.uuid4()))
    time.sleep(60*60*10)

def check_condition_to_start(version_id):
  global product_api
  global object_api

  product_api = Products()
  crawl_api = Crawls()
  object_api = Objects()

  try:
    # Check Crawling process is done
    total_crawl_size = crawl_api.get_size_crawls(version_id)
    crawled_size = crawl_api.get_size_crawls(version_id, status='done')
    if total_crawl_size != crawled_size:
      return False

    # Check Classifying process is done
    total_product_size = product_api.get_size_products(version_id)
    classified_size = product_api.get_size_products(version_id, is_classified=True)
    if total_product_size != classified_size:
      return False

    classifying_queue_size = rconn.llen(REDIS_OBJECT_INDEX_QUEUE)
    if classifying_queue_size > 0:
      return False

    # Check Indexing process is done
    total_object_size = object_api.get_size_objects(version_id)
    indexed_size = object_api.get_size_objects(version_id, is_indexed=True)
    if total_object_size != indexed_size:
      return False

  except Exception as e:
    log.error(str(e))

  return True

def start(rconn):
  while True:
    version_id = get_latest_crawl_version(rconn)
    if version_id is not None:
      ok = check_condition_to_start(version_id)
      if ok is True:
        prepare_objects_to_index(rconn, version_id)
        dispatch(rconn, version_id)
      else:
        time.sleep(60*10)

if __name__ == '__main__':
  try:
    log.info('Start bl-image-index:1')
    Process(target=start, args=(rconn,)).start()
  except Exception as e:
    log.error(str(e))
