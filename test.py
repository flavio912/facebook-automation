import dropbox

# generate access token at https://www.dropbox.com/developers/apps/info/lsrb29ot2xiur0y
db = dropbox.dropbox.Dropbox('')
act = db.users_get_current_account()
dbx = db.with_path_root(dropbox.common.PathRoot.namespace_id(act.root_info.root_namespace_id))

r = dbx.files_list_folder('ns:6764510672')
while r.has_more:
    r = db.files_list_folder_continue(r)

r = db.sharing_list_folders()
for x in r.entries:
    print(f"ns:{x.shared_folder_id} {x.name} - {x.parent_shared_folder_id} - {x.path_lower}")
