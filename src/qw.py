from pullenti.address.AddressService import AddressService

uri = "http://localhost:2222"
AddressService.set_server_connection("http://localhost:2222")


def return_adderess():
    return AddressService
