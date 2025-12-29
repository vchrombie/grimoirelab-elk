# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2023 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#   Quan Zhou <quan@bitergia.com>
#   Miguel Ángel Fernández <mafesan@bitergia.com>
#

import logging

from functools import lru_cache

from grimoirelab_toolkit.datetime import str_to_datetime

from .sortinghat_gelk import MULTI_ORG_NAMES, SortingHat
from .utils import SH_UNKNOWN_VALUE

try:
    from sortinghat.utils import generate_uuid
except ImportError:
    generate_uuid = None

logger = logging.getLogger(__name__)

try:
    from sortinghat.cli.client import SortingHatClientError

    SORTINGHAT_LIBS = True
except ImportError:
    logger.info("SortingHat not available")
    SORTINGHAT_LIBS = False


class Identities:
    def __init__(self, backend_name, sortinghat_db=None):
        self.sh_db = sortinghat_db
        self.backend_name = backend_name
        self.unaffiliated_group = "Unknown"
        self.unknown_gender = "Unknown"

    def get_sh_identity(self, item, identity_field):
        """Empty identity. Real implementation in each data source."""
        identity = {}
        for field in ["name", "email", "username"]:
            identity[field] = None
        return identity

    @staticmethod
    def get_main_enrollments(enrollments):
        """Get the main enrollment given a list of enrollments.
        If the enrollment contains :: the main one is the first part.

        For example:
        - Enrollment: Chaoss::Eng
        - Main: Chaoss

        If there is more than one, it will return ordered alphabetically.
        """
        main_orgs = list(map(lambda x: x.split("::")[0], enrollments))
        main_orgs = sorted(list(set(main_orgs)))

        return main_orgs

    @staticmethod
    def remove_prefix_enrollments(enrollments):
        """Remove the prefix `::` of the enrollments.

        :param enrollments: list of enrollments
        :return: list of enrollments without prefix
        """
        enrolls = [
            enroll.split("::")[1] if "::" in enroll else enroll
            for enroll in enrollments
        ]
        enrolls_unique = sorted(list(set(enrolls)))

        return enrolls_unique

    def __get_item_sh_fields_empty(self, rol, undefined=False):
        """Return a SH identity with all fields to empty_field"""
        # If empty_field is None, the fields do not appear in index patterns
        empty_field = "" if not undefined else "-- UNDEFINED --"
        return {
            rol + "_id": empty_field,
            rol + "_uuid": empty_field,
            rol + "_name": empty_field,
            rol + "_user_name": empty_field,
            rol + "_domain": empty_field,
            rol + "_gender": empty_field,
            rol + "_gender_acc": None,
            rol + "_org_name": empty_field,
            rol + "_bot": False,
            rol + MULTI_ORG_NAMES: [empty_field],
        }

    def get_item_no_sh_fields(
        self, identity, rol, generate_uuid_func, get_identity_domain_func
    ):
        """Create an item with reasonable data when SH is not enabled"""

        username = identity.get("username", "")
        email = identity.get("email", "")
        name = identity.get("name", "")

        if not (username or email or name):
            return self.__get_item_sh_fields_empty(rol)

        uuid = generate_uuid_func(
            self.backend_name, email=email, name=name, username=username
        )
        return {
            rol + "_id": uuid,
            rol + "_uuid": uuid,
            rol + "_name": name,
            rol + "_user_name": username,
            rol + "_domain": get_identity_domain_func(identity),
            rol + "_gender": self.unknown_gender,
            rol + "_gender_acc": None,
            rol + "_org_name": self.unaffiliated_group,
            rol + "_bot": False,
            rol + MULTI_ORG_NAMES: [self.unaffiliated_group],
        }

    def get_individual_fields(
        self,
        individual,
        get_email_domain_func,
        sh_id=None,
        item_date=None,
        rol="author",
    ):
        """Get standard SH fields from a SH identity"""

        eitem_sh = self.__get_item_sh_fields_empty(rol)

        eitem_sh[rol + "_id"] = sh_id
        eitem_sh[rol + "_uuid"] = individual["mk"]

        profile = individual["profile"]
        eitem_sh[rol + "_name"] = profile.get("name", eitem_sh[rol + "_name"])
        email = profile.get("email", None)
        eitem_sh[rol + "_domain"] = get_email_domain_func(email)
        eitem_sh[rol + "_gender"] = profile.get("gender", self.unknown_gender)
        eitem_sh[rol + "_gender_acc"] = profile.get("genderAcc", 0)
        eitem_sh[rol + "_bot"] = profile.get("isBot", False)

        multi_enrolls = self.get_sh_item_multi_enrollments(
            individual["enrollments"], item_date
        )
        main_enrolls = self.get_main_enrollments(multi_enrolls)
        all_enrolls = list(set(main_enrolls + multi_enrolls))
        eitem_sh[rol + MULTI_ORG_NAMES] = self.remove_prefix_enrollments(all_enrolls)
        eitem_sh[rol + "_org_name"] = main_enrolls[0]

        return eitem_sh

    def get_item_sh_fields(
        self,
        get_identity_domain_func,
        get_email_domain_func,
        identity=None,
        item_date=None,
        sh_id=None,
        rol="author",
    ):
        """Get standard SH fields from a SH identity"""

        eitem_sh = self.__get_item_sh_fields_empty(rol)

        if identity:
            sh_item = self.get_sh_item_from_identity(identity, self.backend_name)
            eitem_sh[rol + "_id"] = sh_item.get("id", "")
            eitem_sh[rol + "_uuid"] = sh_item.get("uuid", "")
            eitem_sh[rol + "_name"] = identity.get("name", "")
            eitem_sh[rol + "_user_name"] = identity.get("username", "")
            eitem_sh[rol + "_domain"] = get_identity_domain_func(identity)
        elif sh_id:
            # Use the SortingHat id to get the identity
            sh_item = self.get_sh_item_from_id(sh_id)
            eitem_sh[rol + "_id"] = sh_id
            eitem_sh[rol + "_uuid"] = sh_item.get("uuid", "")
        else:
            # No data to get a SH identity. Return an empty one.
            return eitem_sh

        # If the identity does not exist return an empty identity
        if rol + "_uuid" not in eitem_sh or not eitem_sh[rol + "_uuid"]:
            return self.__get_item_sh_fields_empty(rol, undefined=True)

        if "profile" in sh_item and sh_item["profile"]:
            profile = sh_item["profile"]
            # If name not in profile, keep its old value (should be empty or identity's name field value)
            eitem_sh[rol + "_name"] = profile.get("name", eitem_sh[rol + "_name"])

            email = profile.get("email", None)
            eitem_sh[rol + "_domain"] = get_email_domain_func(email)

            eitem_sh[rol + "_gender"] = profile.get("gender", self.unknown_gender)
            eitem_sh[rol + "_gender_acc"] = profile.get("genderAcc", 0)
            eitem_sh[rol + "_bot"] = profile.get("isBot", False)

        # Ensure we always write gender fields
        if not eitem_sh.get(rol + "_gender"):
            eitem_sh[rol + "_gender"] = self.unknown_gender
            eitem_sh[rol + "_gender_acc"] = 0

        multi_enrolls = self.get_sh_item_multi_enrollments(
            sh_item["enrollments"], item_date
        )
        main_enrolls = self.get_main_enrollments(multi_enrolls)
        all_enrolls = list(set(main_enrolls + multi_enrolls))
        eitem_sh[rol + MULTI_ORG_NAMES] = self.remove_prefix_enrollments(all_enrolls)
        eitem_sh[rol + "_org_name"] = main_enrolls[0]

        return eitem_sh

    @lru_cache(4096)
    def get_sh_item_from_id(self, sh_id):
        """Get all the identity information from SortingHat using the individual id"""

        sh_item = {}

        try:
            individual = self.get_entity(sh_id)
            if not individual:
                msg = "Individual not found given the following id: {}".format(sh_id)
                logger.debug(msg)
                return sh_item
            uuid = individual["mk"]
        except Exception as ex:
            msg = "Error getting individual {} from SortingHat: {}".format(sh_id, ex)
            logger.error(msg)
            return sh_item

        # Fill the information needed with the identity, individual and profile
        sh_item["id"] = sh_id
        sh_item["uuid"] = uuid
        sh_item["profile"] = individual["profile"]
        sh_item["enrollments"] = individual["enrollments"]

        return sh_item

    def get_sh_item_from_identity(self, identity, backend_name):
        identity_tuple = tuple(identity.items())
        sh_item = self.get_sh_item_from_identity_cache(identity_tuple, backend_name)
        return sh_item

    @lru_cache(4096)
    def get_sh_item_from_identity_cache(self, identity_tuple, backend_name):
        """Get a SortingHat item with all the information related with an identity"""
        sh_item = {}
        iden = {}

        # Convert the identity to dict again
        identity = dict((x, y) for x, y in identity_tuple)

        for field in ["email", "name", "username"]:
            iden[field] = identity.get(field)

        if not iden["name"] and not iden["email"] and not iden["username"]:
            logger.warning(
                "Name, email and username are none in {}".format(backend_name)
            )
            return sh_item

        identity_id = self.generate_uuid(
            backend_name,
            email=iden["email"],
            name=iden["name"],
            username=iden["username"],
        )
        iden["uuid"] = identity_id
        iden["id"] = identity_id
        iden["enrollments"] = []

        try:
            individual = self.get_entity(identity_id)
            if not individual:
                msg = "Individual not found given the following identity: {}".format(
                    identity_id
                )
                logger.debug(msg)
                return iden

            for indv_identity in individual["identities"]:
                if indv_identity["uuid"] == identity_id:
                    identity_sh = indv_identity
                    break
            else:
                msg = "Identity {} not found in individual returned by SortingHat.".format(
                    identity
                )
                logger.error(msg)
                return sh_item
        except SortingHatClientError as e:
            msg = "None Identity found {}, identity: {}".format(backend_name, identity)
            logger.error(msg)
            raise SortingHatClientError(e)
        except UnicodeEncodeError:
            msg = "UnicodeEncodeError {}, identity: {}".format(backend_name, identity)
            logger.error(msg)
            return sh_item
        except Exception as ex:
            msg = "Unknown error getting identity from SortingHat, {}, {}, {}".format(
                ex, backend_name, identity
            )
            logger.error(msg)
            return sh_item

        # Fill the information needed with the identity, individual and profile
        sh_item["id"] = identity_sh["uuid"]
        sh_item["uuid"] = individual["mk"]
        sh_item["name"] = identity_sh["name"]
        sh_item["username"] = identity_sh["username"]
        sh_item["email"] = identity_sh["email"]
        sh_item["profile"] = individual["profile"]
        sh_item["enrollments"] = individual["enrollments"]

        return sh_item

    def get_sh_item_multi_enrollments(self, enrollments, item_date_str):
        """Get the enrollments for the uuid when the item was done"""

        enrolls = []
        enrollments = enrollments if enrollments else []

        if enrollments:
            if item_date_str:
                if isinstance(item_date_str, str):
                    item_date = str_to_datetime(item_date_str)
                else:
                    item_date = item_date_str  # Assumes it is already a datetime object
            else:
                item_date = None

            # item_date must be offset-naive (utc)
            if item_date and item_date.tzinfo:
                item_date = (item_date - item_date.utcoffset()).replace(tzinfo=None)

        for enrollment in enrollments:
            group = enrollment["group"]
            if not item_date:
                if group["type"] == "team" and group["parentOrg"]:
                    name = "{}::{}".format(group["parentOrg"]["name"], group["name"])
                else:
                    name = group["name"]
                enrolls.append(name)
            elif (
                str_to_datetime(enrollment["start"]).isoformat()
                <= item_date.isoformat()
                <= str_to_datetime(enrollment["end"]).isoformat()
            ):
                if group["type"] == "team" and group["parentOrg"]:
                    name = "{}::{}".format(group["parentOrg"]["name"], group["name"])
                else:
                    name = group["name"]
                enrolls.append(name)
        if not enrolls:
            enrolls.append(self.unaffiliated_group)

        return enrolls

    def get_item_sh_from_id(
        self,
        eitem,
        get_field_author_func,
        get_field_date_func,
        roles=None,
        individuals=None,
    ):
        # Get the SH fields from the data in the enriched item

        eitem_sh = {}  # Item enriched

        author_field = get_field_author_func()
        if not author_field:
            return eitem_sh
        sh_id_author = None

        if not roles:
            roles = [author_field]

        date = eitem[get_field_date_func()]
        for rol in roles:
            if rol + "_id" not in eitem:
                # For example assignee in github it is usual that it does not appears
                logger.debug(
                    "Enriched index does not include SH ids for {}_id. Can not refresh it.".format(
                        rol
                    )
                )
                continue
            sh_id = eitem[rol + "_id"]
            if not sh_id:
                logger.debug("{}_id is None".format(rol))
                continue
            if rol == author_field:
                sh_id_author = sh_id
            individual = self.find_individual(individuals, sh_id)
            if not individual:
                logger.debug(f"Individual {sh_id} not found.")
                continue
            # Need to pass get_email_domain_func
            # But wait, get_email_domain is just a helper, I can make it part of Identities or pass it.
            # It's simple enough to duplicate or just implement here.
            # I'll implement a static method or instance method here.
            eitem_sh.update(
                self.get_individual_fields(
                    individual=individual,
                    get_email_domain_func=self.get_email_domain,
                    sh_id=sh_id,
                    item_date=date,
                    rol=rol,
                )
            )

        # Add the author field common in all data sources
        rol_author = "author"
        if sh_id_author and author_field != rol_author:
            individual = self.find_individual(individuals, sh_id_author)
            if individual:
                eitem_sh.update(
                    self.get_individual_fields(
                        individual=individual,
                        get_email_domain_func=self.get_email_domain,
                        sh_id=sh_id_author,
                        item_date=date,
                        rol=rol_author,
                    )
                )
            else:
                logger.debug(f"Individual {sh_id_author} not found.")

        return eitem_sh

    def get_item_sh_meta_fields(
        self,
        eitem,
        get_field_date_func,
        roles=None,
        suffixes=None,
        non_authored_prefix=None,
        individuals=None,
    ):
        """Get the SH meta fields from the data in the enriched item."""

        eitem_meta_sh = {}  # Item enriched

        date = eitem[get_field_date_func()]

        for rol in roles:
            if rol + "_uuids" not in eitem:
                continue
            sh_uuids = eitem[rol + "_uuids"]
            if not sh_uuids:
                logger.debug("{}_uuids is None".format(rol))
                continue

            for sh_uuid in sh_uuids:
                individual = self.find_individual(individuals, sh_uuid)
                if not individual:
                    logger.debug(f"Individual {sh_uuid} not found.")
                    continue
                sh_fields = self.get_individual_fields(
                    individual=individual,
                    get_email_domain_func=self.get_email_domain,
                    sh_id=sh_uuid,
                    item_date=date,
                    rol=rol,
                )

                self.add_meta_fields(
                    eitem,
                    eitem_meta_sh,
                    sh_fields,
                    rol,
                    sh_uuid,
                    suffixes,
                    non_authored_prefix,
                )

        return eitem_meta_sh

    @staticmethod
    def find_individual(individuals, sh_id):
        if not individuals:
            return None
        for indiv in individuals:
            if sh_id == indiv["mk"]:
                return indiv
            for identity in indiv["identities"]:
                if sh_id == identity["uuid"]:
                    return indiv
        return None

    def add_meta_fields(
        self, eitem, meta_eitem, sh_fields, rol, uuid, suffixes, non_authored_prefix
    ):
        def add_non_authored_fields(
            author_uuid, uuid, new_eitem, new_list, non_authored_field
        ):
            if author_uuid == uuid:
                non_authored = []
            else:
                non_authored = new_list
            new_eitem[non_authored_field] = non_authored

        for suffix in suffixes:
            field = rol + suffix[:-1]
            if suffix == "_org_names":
                field = rol + "_multi" + suffix

            new_list = sh_fields[field]
            if not isinstance(new_list, list):
                new_list = [new_list]

            try:
                meta_eitem[rol + suffix] += new_list
            except KeyError:
                meta_eitem[rol + suffix] = new_list

            if non_authored_prefix:
                non_authored_field = non_authored_prefix + rol + suffix
                add_non_authored_fields(
                    eitem["author_uuid"], uuid, meta_eitem, new_list, non_authored_field
                )
        return meta_eitem

    def get_users_data(self, item):
        """If user fields are inside the global item dict"""
        if "data" in item:
            users_data = item["data"]
        else:
            # the item is directly the data (kitsune answer)
            users_data = item

        return users_data

    def get_item_sh(
        self,
        item,
        get_field_author_func,
        get_field_date_func,
        get_sh_identity_func,
        get_identity_domain_func,
        roles=None,
        date_field=None,
    ):
        """
        Add sorting hat enrichment fields for different roles

        If there are no roles, just add the author fields.

        """
        eitem_sh = {}  # Item enriched

        author_field = get_field_author_func()

        if not roles:
            roles = [author_field]

        if not date_field:
            item_date = item[get_field_date_func()]
        else:
            item_date = item[date_field]

        users_data = self.get_users_data(item)

        for rol in roles:
            if rol in users_data:
                identity = get_sh_identity_func(item, rol)
                if self.sh_db:
                    sh_fields = self.get_item_sh_fields(
                        get_identity_domain_func,
                        self.get_email_domain,
                        identity,
                        item_date,
                        rol=rol,
                    )
                else:
                    from sortinghat.utils import generate_uuid

                    sh_fields = self.get_item_no_sh_fields(
                        identity, rol, generate_uuid, get_identity_domain_func
                    )
                eitem_sh.update(sh_fields)

                if not eitem_sh[rol + "_org_name"]:
                    eitem_sh[rol + "_org_name"] = SH_UNKNOWN_VALUE

                if not eitem_sh[rol + "_name"]:
                    eitem_sh[rol + "_name"] = SH_UNKNOWN_VALUE

                if not eitem_sh[rol + "_user_name"]:
                    eitem_sh[rol + "_user_name"] = SH_UNKNOWN_VALUE

        # Add the author field common in all data sources
        rol_author = "author"
        if author_field in users_data and author_field != rol_author:
            identity = get_sh_identity_func(item, author_field)
            if self.sh_db:
                sh_fields = self.get_item_sh_fields(
                    get_identity_domain_func,
                    self.get_email_domain,
                    identity,
                    item_date,
                    rol=rol_author,
                )
            else:
                from sortinghat.utils import generate_uuid

                sh_fields = self.get_item_no_sh_fields(
                    identity, rol_author, generate_uuid, get_identity_domain_func
                )
            eitem_sh.update(sh_fields)

            if not eitem_sh["author_org_name"]:
                eitem_sh["author_org_name"] = SH_UNKNOWN_VALUE

            if not eitem_sh["author_name"]:
                eitem_sh["author_name"] = SH_UNKNOWN_VALUE

            if not eitem_sh["author_user_name"]:
                eitem_sh["author_user_name"] = SH_UNKNOWN_VALUE

        return eitem_sh

    @lru_cache(4096)
    def get_entity(self, id):
        return SortingHat.get_entity(self.sh_db, id)

    @lru_cache(4096)
    def is_bot(self, uuid):
        return SortingHat.is_bot(self.sh_db, uuid)

    @lru_cache(4096)
    def get_enrollments(self, uuid):
        return SortingHat.get_enrollments(self.sh_db, uuid)

    @lru_cache(4096)
    def get_unique_identity(self, uuid):
        return SortingHat.get_unique_identity(self.sh_db, uuid)

    @lru_cache(4096)
    def get_uuid_from_id(self, sh_id):
        """Get the SH identity uuid from the id"""
        return SortingHat.get_uuid_from_id(self.sh_db, sh_id)

    def add_sh_identities(self, identities):
        SortingHat.add_identities(self.sh_db, identities, self.backend_name)

    @lru_cache(4096)
    def add_sh_identity_cache(self, identity_tuple):
        """Cache add_sh_identity calls. Identity must be in tuple format"""

        identity = dict((x, y) for x, y in identity_tuple)
        self.add_sh_identity(identity)

    def add_sh_identity(self, identity):
        SortingHat.add_identity(self.sh_db, identity, self.backend_name)

    def generate_uuid(self, source, email=None, name=None, username=None):
        """
        Generate UUID from identity fields.
        Force empty fields to None, the same way add_identity works.
        """
        args = {"email": email, "name": name, "source": source, "username": username}
        args_without_empty = {k: v for k, v in args.items() if v}
        if generate_uuid:
            return generate_uuid(**args_without_empty)
        return None

    def get_email_domain(self, email):
        domain = None
        try:
            domain = email.split("@")[1]
        except (IndexError, AttributeError):
            # logger.warning("Bad email format: %s" % (identity['email']))
            pass
        return domain
