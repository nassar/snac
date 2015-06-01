# To create a new user:
# CREATE ROLE [username] with password '[password]' LOGIN INHERIT;

create role snac_admin noinherit;

grant snac_admin to nassar;
