some helpers for web2py developed in GrandERP

joins_builder 
-------------

Smart joins builder lets you write
```python
build_joins_chain( 'auth_user', 'auth_membership', 'auth_group', 'auth_permission' )
```
instead of
```python
[
  db.auth_membership.on( db.auth_membership.user_id == db.auth_user.id ),
  db.auth_group.on( db.auth_group.id == db.auth_membership.group_id ),
  db.auth_permission.on( db.auth_permission.group_id == db.auth_group.id ),
]
```

for more examples see  ```controllers/plugins_joins_builder.py```

search_form
-----------
helps buid SEARCH FORM  so, that 
entered values are used to construct search_query

inspiration came from grid search (which itself is not well suitable for end users)..

Search form  has extra layer when defining  Fields -- it defines:
- comparison operator
- Expression -- that is compared to entered value
- some optional params.. 

Example:
```python
search = SearchForm(
    queryFilter( db.auth_user.first_name),
    queryFilter( db.auth_user.last_name),
    queryFilter( db.auth_user.email )
)

data = db((db.auth_user.id > 0) & search.query).select( db.auth_user.ALL )  
```
for more examples see  ```controllers/plugins_search_form.py```
 
