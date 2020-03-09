from google.cloud import datastore

import lmsdata # the classes we defined

# these entity name values are defined within our index.yaml file as "kind"
# kind is they type of entity that for the query 
# definition here: https://cloud.google.com/appengine/docs/flexible/go/configuring-datastore-indexes-with-index-yaml
_USER_ENTITY = 'LmsUser' 
_COURSE_ENTITY = 'LmsCourse'
_LESSON_ENTITY = 'LmsLesson'


def _get_client():
    """Build a datastore client."""
    # documentation on datastore: https://googleapis.dev/python/datastore/latest/client.html
    # definition: Convenience wrapper for invoking APIs/factories w/ a project.

    return datastore.Client()


def log(msg):
    """Log a simple message."""
    # logging information to Log Viewer
    # here: https://console.cloud.google.com/logs/viewer?resource=gae_app&_ga=2.13944496.640558531.1583523379-933297884.1578420671
    print('lmsdatastore: %s' % msg)


def _load_key(client, entity_type, entity_id=None, parent_key=None):
    """Load a datastore key using a particular client, and if known, the ID.
    Note that the ID should be an int - we're allowing datastore to generate
    them in this example."""

    key = None
    if entity_id:
        # client.key is a proxy to google.cloud.datastore.key.Key
        # documentation here: https://googleapis.dev/python/datastore/latest/keys.html#google.cloud.datastore.key.Key
        key = client.key(entity_type, entity_id, parent=parent_key)
    else:
        # this will generate an ID
        key = client.key(entity_type)
    return key # this is an int


def _load_entity(client, entity_type, entity_id, parent_key=None):
    """Load a datstore entity using a particular client, and the ID."""

    key = _load_key(client, entity_type, entity_id, parent_key) # getting our int key here
    entity = client.get(key)
    log('retrieved entity for ' + str(entity_id))
    return entity # this is an Entity object (google.cloud.datastore.entity.Entity) or NoneType
    # documentation here: https://googleapis.dev/python/datastore/latest/entities.html
    # You can the set values on the entity just like you would on any other dictionary. (e.g., lesson_entity['title'] = 'blah')
    # Note: Lesson object's title is defined in lmsdata.py (but not our index.yaml)

def _course_from_entity(course_entity): # input: Entity
    """Translate the Course entity to a regular old Python object."""

    code = course_entity.key.name # this is a string version of the key
    name = course_entity['name'] # acessing Entity as a dictionary element to pull out name value (for us to use within our object)
    desc = course_entity['description']
    course = lmsdata.Course(code, name, desc, []) # creating our object, the [] will be for lessons 
    log('built object from course entity: ' + str(code))
    return course # output: python Course object


def _lesson_from_entity(lesson_entity, include_content=True):
    """Translate the Lesson entity to a regular old Python object."""

    lesson_id = lesson_entity.key.id # this is an int version of the key
    title = lesson_entity['title'] # You can the set values on the entity just like you would on any other dictionary.
    content = ''
    if include_content:
        content = lesson_entity['content']
    lesson = lmsdata.Lesson(lesson_id, title, content) # where our classes from lmsdata.py come into play
    log('built object from lesson entity: ' + str(title)) # logging to log viewer
    return lesson # spits out a Python Lesson object


def load_course(course_code): # inputing the course code to get information from datastore
    """Load a course from the datastore, based on the course code."""

    log('loading course: ' + str(course_code))
    client = _get_client() # gets you a datastore client
    course_entity = _load_entity(client, _COURSE_ENTITY, course_code) # loads the Entity from the datastore
    log('loaded course: ' + course_code)
    course = _course_from_entity(course_entity) # translated the Entity to a Course python object
    query = client.query(kind=_LESSON_ENTITY, ancestor=course_entity.key) # preps a query that's going to look at Lesson Entity types
    for lesson in query.fetch(): # gets us the Lesson information from datastore
        course.add_lesson(_lesson_from_entity(lesson, False)) # adds the Lessons to the course; making sure to translate lesson entities into Lesson python objects
    log('loaded lessons: ' + str(len(course.lessons)))
    return course # returns python Course object


def load_courses():
    """Load all of the courses."""

    client = _get_client()
    
    # .query() = Proxy to google.cloud.datastore.query.Query
    # definition: A Query against the Cloud Datastore.
    # query documentation: https://googleapis.dev/python/datastore/latest/queries.html
    q = client.query(kind=_COURSE_ENTITY)
    
    # minus sign before = descending order
    # Prepend - to a field name to sort it in descending order
    q.order = ['-name']
    result = []
    for course in q.fetch(): # q.fetch = Returns the iterator for the query.
        result.append(course)
    return result # list of all of the elements within the _COURSE_ENTITY query within the datastore


def load_lesson(course_code, lesson_id):
    """Load a lesson under the given course code."""

    log('loading lesson detail: ' + str(course_code) + ' / ' + str(lesson_id))
    client = _get_client()
    parent_key = _load_key(client, _COURSE_ENTITY, course_code) # returns int
    lesson_entity = _load_entity(client, _LESSON_ENTITY, lesson_id, parent_key) # loads the entity from the datastore
    return _lesson_from_entity(lesson_entity) # method translates to python object, so lesson_entity becomes a Lesson object


def load_user(username, passwordhash): # note: index.yaml and our User object do not contain passwordhash (it's only in datastore)
    """Load a user based on the passwordhash; if the passwordhash doesn't match
    the username, then this should return None."""

    client = _get_client() # get the datastore client
    q = client.query(kind=_USER_ENTITY) # prep a query that looks at user entities
    
    # .add_filter('<property>', '<operator>', <value>)
    # Filter the query based on a property name, operator and a value.
    # Documentation: https://googleapis.dev/python/datastore/latest/queries.html#google.cloud.datastore.query.Query.add_filter
    q.add_filter('username', '=', username) # must equal un 
    q.add_filter('passwordhash', '=', passwordhash) # must equal pwhash
    
    for user in q.fetch(): # fetch the information on the user
        # get the information from the datastore (accessing as one accesses a dictionary)
        # use that information as a parameter to insert in our python User object
        # return object
        return lmsdata.User(user['username'], user['email'], user['about']) 
    return None # if info doesn't exist return None


def load_about_user(username):
    """Return a string that represents the "About Me" information a user has
    stored."""

    user = _load_entity(_get_client(), _USER_ENTITY, username)
    if user:
        return user['about']
    else:
        return ''


def load_completions(username):
    """Load a dictionary of coursecode => lessonid => lesson name based on the
    lessons the user has marked complete."""

    client = _get_client() # get a datastore client
    user_entity = _load_entity(client, _USER_ENTITY, username) # load a user entity based on the username argument
    courses = dict() # instantiate a dictionary
    for completion in user_entity['completions']: #load in completed courses from datastore
        lesson_entity = client.get(completion)
        course_entity = client.get(completion.parent)
        code = course_entity.key.name
        if code not in courses:
            courses[code] = dict()
        courses[code][completion.id] = lesson_entity['title']
    return courses


def save_user(user, passwordhash):
    """Save the user details to the datastore."""

    client = _get_client() # get datastore client
    entity = datastore.Entity(_load_key(client, _USER_ENTITY, user.username)) # load information relating to the entity
    entity['username'] = user.username
    entity['email'] = user.email
    entity['passwordhash'] = passwordhash # this is only accessible within the datastore
    entity['about'] = ''
    entity['completions'] = [] # these are only accessible within the datastore
    client.put(entity) # update entity within datastore


def save_about_user(username, about):
    """Save the user's about info to the datastore."""

    client = _get_client() # get datastore client
    user = _load_entity(client, _USER_ENTITY, username) # load information from users
    user['about'] = about # input parameter about into user entity, accessing as one accesses a dictionary key
    client.put(user) # save to datastore


def save_completion(username, coursecode, lessonid):
    """Save a completion (i.e., mark a course as completed in the
    datastore)."""

    client = _get_client() # get a datastore client
    course_key = _load_key(client, _COURSE_ENTITY, coursecode) # load key information about course
    lesson_key = _load_key(client, _LESSON_ENTITY, lessonid, course_key) # load key information about lesson
    user_entity = _load_entity(client, _USER_ENTITY, username) # load user entity information from datastore
    completions = set() # instantiate a set data structure
    for completion in user_entity['completions']: # access the datastore and pull down all completed courses
        completions.add(completion) # add completion to the complesions set
    if lesson_key not in completions: # if the lesson was not in competions
        user_entity['completions'].append(lesson_key) # add the lesson key within the datastore
    client.put(user_entity) # save the user entity

#######################################################################
###### Testing, Updating info to see if python/datastore work #########
#######################################################################

def create_data():
    """You can use this function to populate the datastore with some basic
    data."""

    client = _get_client() # get a datastore client
    
    ####### User
    # create a test user 
    entity = datastore.Entity(client.key(_USER_ENTITY, 'testuser'),
                              exclude_from_indexes=[])
    # update information
    entity.update({
        'username': 'testuser',
        'passwordhash': '',
        'email': '',
        'about': '',
        'completions': [],
    })
    client.put(entity) # save information to datastore

    ####### Course
    # create a fake course as an entity, Course01
    entity = datastore.Entity(client.key(_COURSE_ENTITY, 'Course01'),
                              exclude_from_indexes=['description', 'code'])
    # add information about Course01
    entity.update({
        'code': 'Course01',
        'name': 'First Course',
        'description': 'This is a description for a test course.  In the \
future, real courses will have lots of other stuff here to see that will tell \
you more about their content.',
    })
    client.put(entity) # save information to datastore 
     
    # create fake course as entity, Course02
    entity = datastore.Entity(client.key(_COURSE_ENTITY, 'Course02'),
                              exclude_from_indexes=['description', 'code'])
    # update information
    entity.update({
        'code': 'Course02',
        'name': 'Second Course',
        'description': 'This is also a course description, but maybe less \
wordy than the previous one.'
    })
    client.put(entity) # save information to datastore
    
    ####### Lessons  
    ## Course01 - L1, create a lesson entity under Course
    entity = datastore.Entity(client.key(_COURSE_ENTITY,
                                         'Course01',
                                         _LESSON_ENTITY),
                              exclude_from_indexes=['content', 'title'])
    
    # update information about Lesson, as you would a dictionary
    entity.update({
        'title': 'Lesson 1: The First One',
        'content': 'Imagine there were lots of video content and cool things.',
    })
    client.put(entity) # save information to datastore
    
    ## Course01 - L2, create a lesson entity under Course
    entity = datastore.Entity(client.key(_COURSE_ENTITY,
                                         'Course01',
                                         _LESSON_ENTITY),
                              exclude_from_indexes=['content', 'title'])
    # update information about Lesson, as you would a dictionary
    entity.update({
        'title': 'Lesson 2: Another One',
        'content': '1<br>2<br>3<br>4<br>5<br>6<br>7<br>8<br>9<br>10<br>11',
    })
    client.put(entity) # save information to datastore
    
    ## Course02 - L1, create a lesson entity under Course
    entity = datastore.Entity(client.key(_COURSE_ENTITY,
                                         'Course02',
                                         _LESSON_ENTITY),
                              exclude_from_indexes=['content', 'title'])
    
    # update information about Lesson, as you would a dictionary
    entity.update({
        'title': 'Lesson 1: The First One, a Second Time',
        'content': '<p>Things</p><p>Other Things</p><p>Still More Things</p>',
    })

    client.put(entity) # save information to datastore
    
    ## Course02 - L2, create a lesson entity under Course
    entity = datastore.Entity(client.key(_COURSE_ENTITY,
                                         'Course02',
                                         _LESSON_ENTITY),
                              exclude_from_indexes=['content', 'title'])
    
    # update information about Lesson, as you would a dictionary
    entity.update({
        'title': 'Lesson 2: Yes, Another One',
        'content': '<ul><li>a</li><li>b</li><li>c</li><li>d</li><li></ul>',
    })
    client.put(entity) # save information to datastore
