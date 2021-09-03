
from semql.to_text.attribute import *
from semql.to_text.table import Table, PartialRelation, RelationTable


class LiveIn(VerbPhrase):

    def __init__(self, name, is_default=False):
        super(LiveIn, self).__init__(
            name=name,
            auxiliary="are",
            participle="living",
            preposition="in",
            is_default=is_default,
        )


class LiveAt(VerbPhrase):

    def __init__(self, name, is_default=False):
        super(LiveAt, self).__init__(
            name=name,
            auxiliary="are",
            participle="living",
            preposition="at",
            is_default=is_default,
        )


CHINOOK_DB = {
    "tables": {
        "Album": PartialRelation(
            name="Album",
            attributes={
                "AlbumId": PrimaryKey(),
                "Title": Name(is_default=True),
                "ArtistId": ForeignKey("ArtistId", target_table="Artist"),
            },
            templates={
                "ArtistId": {
                    "Artist": ["$Artist", "who released", "$Album"],
                    "Album": ["$Album", "by", "$Artist"],
                },
            },
        ),
        "Artist": Table(
            name="Artist",
            attributes={
                "ArtistId": PrimaryKey(),
                "Name": Name(is_default=True),
            },
        ),
        "Customer": PartialRelation(
            name="Customer",
            attributes={
                "CustomerId": PrimaryKey(),
                "FirstName": Text("first name"),
                "LastName": Text("last name", is_default=True),
                "Company": Text("company"),
                "Address": LiveAt(name="street"),
                "City": LiveIn(name="city"),
                "State": LiveIn(name="state"),
                "Country": LiveIn(name="state"),
                "PostalCode": Text(name="postal code"),
                "Phone": Text(name="phone number"),
                "Fax": Text(name="fax number"),
                "Email": Text(name="email"),
                "SupportRepId": ForeignKey("SupportRepId", target_table="Employee"),
            },
            templates={
                "SupportRepId": {
                    "Customer": [
                        "$Customer", "who get support from", "$Employee",
                    ],
                    "Employee": [
                        "$Employee", "who provide support for", "$Customer",
                    ],
                }
            },
        ),
        # Note the employee table has a foreign key referring to itself we do not
        # support this at the moment, therefore we treat it as an entity instead
        # of partial relation
        "Employee": Table(
            name="Employee",
            attributes={
                "EmployeeId": PrimaryKey(),
                "LastName": Text("last name", is_default=True),
                "FirstName": Text("first name"),
                "Title": Text(name="job title"),
                "ReportsTo": ForeignKey("ReportsTo", target_table="Employee"),  # / ! \ assumed to be unused
                "BirthDate": Date(name="birth date", participle="born"),
                "HireDate": Date(name="hire date", participle="hired"),
                "Address": LiveAt(name="street"),
                "City": LiveIn(name="city"),
                "State": LiveIn(name="state"),
                "Country": LiveIn(name="state"),
                "PostalCode": Text(name="postal code"),
                "Phone": Text(name="phone number"),
                "Fax": Text(name="fax number"),
                "Email": Text(name="email"),
            },
        ),
        "Genre": Table(
            name="Genre",
            attributes={
                "GenreId": PrimaryKey(),
                "Name": Name(is_default=True),
            }
        ),
        "Invoice": PartialRelation(
            name="Invoice",
            attributes={
                "InvoiceId": PrimaryKey(),
                "CustomerId": ForeignKey("CustomerId", target_table="Customer"),
                "InvoiceDate": Date("invoice date", participle='created', is_default=True),
                "BillingAddress": Text(name="billing street"),
                "BillingCity": Text(name="billing city"),
                "BillingState": Text(name="billing state"),
                "BillingCountry": Text(name="billing country"),
                "BillingPostalCode": Text(name="billing postal code"),
                "Total": Numeric(name="billing total", unit="dollar"),
            },
            templates={
                "CustomerId": {
                    "Invoice": ["$Invoice", "billed to", "$Customers"],
                    "Customer": [
                        "$Customer", "who were billed for", "$Invoice"],
                }
            },
        ),
        "InvoiceLine": PartialRelation(
            name="InvoiceLine",
            pretty_name="invoice line",
            attributes={
                "InvoiceLineId": PrimaryKey(),
                "InvoiceId": ForeignKey("InvoiceId", target_table="Invoice"),
                "TrackId": ForeignKey("TrackId", target_table="Track"),
                "UnitPrice": Numeric(name="price", unit="dollar"),
                "Quantity": Numeric(name="quantity"),
            },
            templates={
                "InvoiceId": {
                    "InvoiceLine": [
                        "$InvoiceLine", "which are part of", "$Invoice",
                    ],
                    "Invoice": ["$Invoice", "containing", "$InvoiceLine"],
                },
                "TrackId": {
                    "InvoiceLine": ["$InvoiceLine", "for", "$Track"],
                    "Track": ["$Track", "on", "$InvoiceLine"],
                },
            },
        ),
        "MediaType": Table(
            name="MediaType",
            pretty_name="media type",
            attributes={
                "MediaTypeId": PrimaryKey(),
                "Name": Name(is_default=True),
            },
        ),
        "Playlist": Table(
            name="Playlist",
            attributes={
                "PlaylistId": PrimaryKey(),
                "Name": Name(is_default=True),
            },
        ),
        "PlaylistTrack": RelationTable(
            name="PlaylistTrack",
            attributes={
                "PlaylistId": ForeignKey("PlaylistId", target_table="Playlist"),
                "TrackId": ForeignKey("TrackId", target_table="Track"),
            },
            relation_components={'Playlist', 'Track'},
            templates={
                "Playlist": ["$Playlist", "containing", "$Track"],
                "Track": ["$Track", "on", "$Playlist"],
            },
        ),
        "Track": PartialRelation(
            name="Track",
            attributes={
                "TrackId": PrimaryKey(),
                "Name": Name(is_default=True),
                "AlbumId": ForeignKey("AlbumId", target_table="Album"),
                "MediaTypeId": ForeignKey("MediaTypeId", target_table="MediaType"),
                "GenreId": ForeignKey("GenreId", target_table="Genre"),
                "Composer": Text("composer"),
                "Milliseconds": Numeric(name="runtime", unit="millisecond"),
                "Bytes": Numeric(name="size", unit="byte"),
                "UnitPrice": Numeric(name="price", unit="dollar"),
            },
            templates={
                "AlbumId": {
                    "Track": ["$Track", "on", "$Album"],
                    "Album": ["$Album", "containing", "$Track"],
                },
                "MediaTypeId": {
                    "Track": ["$Track", "on", "$MediaType"],
                    "MediaType": ["$MediaType", "comprising", "$Track"],
                },
                "GenreId": {
                    "Track": ["$Track", "with", "$Genre"],
                    "Genre": ["$Genre", "comprising", "$Track"],
                },
            },
        ),
    },
}
