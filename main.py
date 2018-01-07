from __future__ import print_function
import uuid

import os
from multiprocessing import Process

import redis
import time
from bluelens_spawning_pool import spawning_pool
from stylelens_product.products import Products
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
REDIS_SEARCH_RESTART_QUEUE = 'bl:search:restart:queue'
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

VECTOR_SEARCH_HOST = os.environ['VECTOR_SEARCH_HOST']
VECTOR_SEARCH_PORT = os.environ['VECTOR_SEARCH_PORT']

MAX_PROCESS_NUM = int(os.environ['MAX_PROCESS_NUM'])

DB_PRODUCT_HOST = os.environ['DB_PRODUCT_HOST']
DB_PRODUCT_PORT = os.environ['DB_PRODUCT_PORT']
DB_PRODUCT_USER = os.environ['DB_PRODUCT_USER']
DB_PRODUCT_PASSWORD = os.environ['DB_PRODUCT_PASSWORD']
DB_PRODUCT_NAME = os.environ['DB_PRODUCT_NAME']

DB_OBJECT_HOST = os.environ['DB_OBJECT_HOST']
DB_OBJECT_PORT = os.environ['DB_OBJECT_PORT']
DB_OBJECT_NAME = os.environ['DB_OBJECT_NAME']
DB_OBJECT_USER = os.environ['DB_OBJECT_USER']
DB_OBJECT_PASSWORD = os.environ['DB_OBJECT_PASSWORD']

DB_IMAGE_HOST = os.environ['DB_IMAGE_HOST']
DB_IMAGE_PORT = os.environ['DB_IMAGE_PORT']
DB_IMAGE_NAME = os.environ['DB_IMAGE_NAME']
DB_IMAGE_USER = os.environ['DB_IMAGE_USER']
DB_IMAGE_PASSWORD = os.environ['DB_IMAGE_PASSWORD']

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
  pool.addContainerEnv(container, 'MAX_PROCESS_NUM', str(MAX_PROCESS_NUM))
  pool.addContainerEnv(container, 'RELEASE_MODE', RELEASE_MODE)
  pool.addContainerEnv(container, 'VECTOR_SEARCH_HOST', VECTOR_SEARCH_HOST)
  pool.addContainerEnv(container, 'VECTOR_SEARCH_PORT', VECTOR_SEARCH_PORT)
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
  pool.addContainerEnv(container, 'DB_PRODUCT_HOST', DB_PRODUCT_HOST)
  pool.addContainerEnv(container, 'DB_PRODUCT_PORT', DB_PRODUCT_PORT)
  pool.addContainerEnv(container, 'DB_PRODUCT_USER', DB_PRODUCT_USER)
  pool.addContainerEnv(container, 'DB_PRODUCT_PASSWORD', DB_PRODUCT_PASSWORD)
  pool.addContainerEnv(container, 'DB_PRODUCT_NAME', DB_PRODUCT_NAME)
  pool.addContainerEnv(container, 'DB_IMAGE_HOST', DB_IMAGE_HOST)
  pool.addContainerEnv(container, 'DB_IMAGE_PORT', DB_IMAGE_PORT)
  pool.addContainerEnv(container, 'DB_IMAGE_USER', DB_IMAGE_USER)
  pool.addContainerEnv(container, 'DB_IMAGE_PASSWORD', DB_IMAGE_PASSWORD)
  pool.addContainerEnv(container, 'DB_IMAGE_NAME', DB_IMAGE_NAME)
  pool.setContainerImage(container, 'bluelens/bl-image-indexer:' + RELEASE_MODE)
  pool.setContainerImagePullPolicy(container, 'Always')
  pool.addContainer(container)
  pool.setRestartPolicy('Never')
  pool.spawn()

def remove_prev_pods():
  pool = spawning_pool.SpawningPool()
  pool.setServerUrl(REDIS_SERVER)
  pool.setServerPassword(REDIS_PASSWORD)
  data = {}
  data['namespace'] = RELEASE_MODE
  data['key'] = 'group'
  data['value'] = 'bl-image-indexer'
  pool.delete(data)
  time.sleep(60)

def prepare_objects_to_index(rconn, version_id):
  global object_api
  offset = 0
  limit = 300
  log.info('prepare_objects_to_index')

  # rconn.delete(REDIS_IMAGE_INDEX_QUEUE)
  # remove_prev_pods()
  try:
    while True:
      res = object_api.get_object_ids(version_id=version_id,
                                   is_indexed=True,
                                   image_indexed=False,
                                   offset=offset,
                                   limit=limit)
      log.debug("Got " + str(len(res)) + ' objects')
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

  if size >= MAX_PROCESS_NUM and size < MAX_PROCESS_NUM*10:
    for i in range(20):
      spawn(str(uuid.uuid4()))

  elif size >= MAX_PROCESS_NUM*100:
    for i in range(30):
      spawn(str(uuid.uuid4()))

def check_condition_to_start(version_id):
  global product_api
  global object_api

  product_api = Products()
  object_api = Objects()

  try:
    # Check Classifying process is done
    total_product_size = product_api.get_size_products(version_id)
    classified_size = product_api.get_size_products(version_id, is_classified=True)
    if total_product_size != classified_size:
      return False

    # Check Object Indexing process is done
    total_object_size = object_api.get_size_objects(version_id)
    object_indexed_size = object_api.get_size_objects(version_id, is_indexed=True)
    if total_object_size != object_indexed_size:
      return False

    # Check Image Indexing process is done
    # total_object_size = object_api.get_size_objects(version_id)
    image_indexed_size = object_api.get_size_objects(version_id, is_indexed=True, image_indexed=True)
    if total_object_size == image_indexed_size:
      return False

    queue_size = rconn.llen(REDIS_IMAGE_INDEX_QUEUE)
    if queue_size != 0:
      return False

  except Exception as e:
    log.error(str(e))

  return True

def restart_bl_search_vector(rconn):
  rconn.lpush(REDIS_SEARCH_RESTART_QUEUE, '1')
  time.sleep(60*10)
  # time.sleep(60*1)

def start(rconn):
  global product_api
  global object_api
  product_api = Products()
  object_api = Objects()
  while True:
    version_id = get_latest_crawl_version(rconn)
    if version_id is not None:
      log.info("check_condition_to_start")
      ok = check_condition_to_start(version_id)
      log.info("check_condition_to_start: " + str(ok))
      if ok is True:
        restart_bl_search_vector(rconn)
        prepare_objects_to_index(rconn, version_id)
        dispatch(rconn, version_id)
      else:
        time.sleep(60*10)

if __name__ == '__main__':
  try:
    log.info('Start bl-image-index:2')
    Process(target=start, args=(rconn,)).start()
  except Exception as e:
    log.error(str(e))
