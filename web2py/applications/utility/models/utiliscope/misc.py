import types

# ============== Database Helpers =============
for table in db.tables:
    def first(self):
        return db(self.id>0).select(orderby=self.id, limitby=(0,1)).first()
    def last(self, N=1):
        rows = db(self.id>0).select(orderby=~self.id, limitby=(0,N))
        return rows.first() if N==1 else rows
    def all(self, *cols, **rest):
        return db(self.id>0).select(*cols, **rest)
    def count(self):
        return db(self.id>0).count()
    t = db[table]
    t.first = types.MethodType(first, t)
    t.last = types.MethodType(last, t)
    t.all = types.MethodType(all, t)
    # Count causing an error
    #t.count = types.MethodType(count, t)
def store_get(key):
    r = db(db.store.key==key).select().first()
    return r and fromjson(r.value)
def store_set(key, value):
    # update_or_insert doesn't work in old web2pys... cause of a bug...
    #return db.store.update_or_insert(key=key, value=tojson(value))
    # So I wrote my own:
    value = tojson(value)
    record = db.store(db.store.key==key)
    return record.update_record(value=value) \
        if record else db.store.insert(key=key, value=value)
def store_append(key, value):
    x = store_get(key) or []; x.append(value)
    store_set(key, x)

# =========
def get_one(query):
    '''
    Deprecated -- this function can mostly be replaced with
       web2py's built-in db.table(field=field) syntax now
    '''
    result = db(query).select()
    assert len(result) <= 1, "GAH Get_one called when there's MORE than one!"
    return result[0] if len(result) == 1 else None
def get_or_make_one(query, table, default_values):
    result = get_one(query)
    if result:
        return result
    else:
        table.insert(**default_values)
        return get_one(query)
def update_or_insert_one(table, column, equalto, values):
    result = get_one(table[column] == equalto)
    if result:
        result.update_record(**values)
    else:
        values[column] = equalto
        table.insert(**values)


# ============== Turk Fees and Price Calculation =============
def add_turk_fees(hit_price):
    return max(.005, hit_price + hit_price*.1)
def calc_study_price (num_hits, prices):
    min = calc_study_price_min(num_hits, prices)
    max = calc_study_price_max(num_hits, prices)
    mean = sum([add_turk_fees(x) for x in prices]) * (num_hits/len(prices))
    print "Between $%.2f (balanced) and $%.2f (max).  Min is $%.2f." % (mean, max, min)
    #return mean
def calc_study_price_max (num_hits, prices):
    return add_turk_fees(max(prices)) * (num_hits)
def calc_study_price_min (num_hits, prices):
    return add_turk_fees(min(prices)) * (num_hits)
# def calc_study_price(number, start, stop, increment):
#     def arith(start, stop, increment):
#         return sum([x for x in range(start, stop, increment)])
#     return arith(start, stop, increment) * 

def calc_real_study_price(study):
    finishes = db((db.actions.study==study)
                  & (db.actions.action=='finished')).select(db.actions.condition)
    prices = (add_turk_fees(load_condition(f.condition)['price'])
              for f in finishes)
    return sum(prices)

def calc_real_study_wage(study, wage=10):
    """Return fair wage based on median HIT time."""
    import numpy as np
    times = db(
        (db.actions.study == study)
        & (db.actions.action == 'finished')
        & (db.actions.hitid == db.assignments.hitid)
        & (db.actions.workerid == db.assignments.workerid)
        & (db.actions.assid == db.assignments.assid)
    ).select(
        db.assignments.accept_time,
        db.actions.time,
        distinct=db.assignments.assid,
    )
    diffs = [(row.actions.time - row.assignments.accept_time).total_seconds() for row in times if row.actions.time and row.assignments.accept_time]
    median_secs = np.median(diffs)
    return median_secs / 3600 * wage

def balance():
    balance = float(turk.get(turk.balance(), 'Amount'))
    to_pay = sum((float(a.amount) for a in db().select(db.bonus_queue.amount)))
    print ('We have a little less than $%.2f - $%.2f = $%.2f' %
           (balance, to_pay, balance - to_pay))

# ============== Studies =============
def study_feedback(study):
    return db((db.feedback.hitid == db.hits.hitid)
              & (db.hits.study == study)).select(db.feedback.message,
                                                 db.feedback.time,
                                                 db.hits.hitid,
                                                 db.feedback.workerid,
                                                 orderby=~db.feedback.time)

def print_hits():
    for study in db().select(db.studies.ALL):
        print study.name
        for h in study.hits.select():
            print '   ', h.launch_date, h.status, h.hitid, h.price, h.othervars

def print_studies(more=False):
    for study in db().select(db.studies.ALL, orderby=db.studies.id):
        print '%d\t%d\t%s%s' % (study.id,
                                db(db.hits.study == study).count(),
                                study.name,
                                '\t' + study.description if more else '')


def open_hits():
    return db(db.hits.status.belongs(('open', 'getting done'))).select()

def num_open_hits():
    return db((db.hits.status == 'open')
              |(db.hits.status == 'getting done')).count()

def print_open_hits():
    print db((db.hits.status == 'open')
             |(db.hits.status == 'getting done')).select(db.hits.status,
                                                         db.hits.task,
                                                         db.hits.launch_date)

def expire_open_hits(hits=None):
    bad_count = 0
    hits = hits or open_hits()
    for i,hit in enumerate(hits):
        if i % 100 == 0:
            log('Expiring hit #%d' % i)
        try:
            turk.expire_hit(hit.hitid)
        except:
            bad_count += 1
    print('FAILED to expire %d/%d hits!' % (bad_count, len(hits)))

def cancel_unlaunched_hits():
    n = db(db.hits.status == 'unlaunched').update(status='launch canceled')
    db.commit()
    log('Canceled %s unlaunched hits' % n)



# ============== Experimental Conditions =============
last_study = None
last_conditions = None
def study_conditions_with(study, var, val):
    global last_conditions, last_study
    if last_study != study:
        last_study = study
        last_conditions = available_conditions(study)
    return [c for c in last_conditions if fromjson(c.json)[var] == val]

def study_conditions_by_var(study, var):
    result = {}
    vars_vals = experimental_vars_vals(study)
    for val in vars_vals[var]:
        result[val] = study_conditions_with(study, var, val)
    return result

def conditions_query(table, conditions):
    query = table.id < 0
    for c in conditions:
        query = (query | (table.condition == c))
    return query

def pretty_condition(study, condition):
    c = fromjson(condition.json)
    items = c.items()
    def pretty(item):
        if item[0] == 'price':
            return '$%.2f' % item[1]
        elif type(item[1]) == type(.234):
            return '%s %.2f' % (item[0], item[1])
        else:
            return '%s %s' % (item[0], item[1])

    evs = set(experimental_vars(study))

    return ', '.join([pretty(i) for i in items if i[0] in evs])

def pretty_int2(x):
    import locale
    locale.setlocale(locale.LC_ALL, 'en_US')
    return locale.format("%d", x, grouping=True)

def pretty_int(x):
    if type(x) not in [type(0), type(0L)]:
        raise TypeError("Parameter must be an integer.")
    if x < 0:
        return '-' + intWithCommas(-x)
    result = ''
    while x >= 1000:
        x, r = divmod(x, 1000)
        result = ",%03d%s" % (r, result)
    return "%d%s" % (x, result)


# ============== Random Help =============
def hit_serve_url(task):
    url = 'localhost' if (sandbox_serves_from_localhost_p and sandboxp) else external_server_url
    return 'https://%s:%s/%s?live' % (url, external_server_port, task)
def url(f,args=[],vars={}): return URL(r=request,f=f,args=args,vars=vars)
