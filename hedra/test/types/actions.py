from hedra.core.engines.types.common import Request, Response
from hedra.core.engines.types.playwright import Command, Result


MercuryRequest = type[Request]
MercuryResponse = type[Response]
MercuryCommand = type[Command]
MercuryResult = type[Result]