package protocol

import "fmt"

type RequestKeyVersion struct {
	Length     int32
	ApiKey     int16
	ApiVersion int16
}

func (r *RequestKeyVersion) decode(pd packetDecoder) (err error) {
	r.Length, err = pd.getInt32()
	if err != nil {
		return err
	}
	if r.Length <= 4 {
		return PacketDecodingError{fmt.Sprintf("message of length %d too small", r.Length)}
	}
	r.ApiKey, err = pd.getInt16()
	if err != nil {
		return err
	}
	r.ApiVersion, err = pd.getInt16()
	return err
}

// Determine response header version. Function returns -1 for unknown api key.
// See also public short responseHeaderVersion(short _version) in kafka/clients/src/generated/java/org/apache/kafka/common/message/ApiMessageType.java
func (r *RequestKeyVersion) ResponseHeaderVersion() int16 {
	switch r.ApiKey {
	case 0:
		return 0
	case 1:
		return 0
	case 2:
		return 0
	case 3:
		if r.ApiVersion >= 9 {
			return 1
		} else {
			return 0
		}
	case 4:
		if r.ApiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 5:
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 6:
		if r.ApiVersion >= 6 {
			return 1
		} else {
			return 0
		}
	case 7:
		if r.ApiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 8:
		if r.ApiVersion >= 8 {
			return 1
		} else {
			return 0
		}
	case 9:
		if r.ApiVersion >= 6 {
			return 1
		} else {
			return 0
		}
	case 10:
		if r.ApiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 11:
		if r.ApiVersion >= 6 {
			return 1
		} else {
			return 0
		}
	case 12:
		if r.ApiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 13:
		if r.ApiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 14:
		if r.ApiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 15:
		if r.ApiVersion >= 5 {
			return 1
		} else {
			return 0
		}
	case 16:
		if r.ApiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 17:
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 18:
		return 0
	case 19:
		if r.ApiVersion >= 5 {
			return 1
		} else {
			return 0
		}
	case 20:
		if r.ApiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 21:
		return 0
	case 22:
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 23:
		return 0
	case 24:
		return 0
	case 25:
		return 0
	case 26:
		return 0
	case 27:
		return 0
	case 28:
		if r.ApiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 29:
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 30:
		return 0
	case 31:
		return 0
	case 32:
		return 0
	case 33:
		return 0
	case 34:
		return 0
	case 35:
		return 0
	case 36:
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 37:
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 38:
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 39:
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 40:
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 41:
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 42:
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 43:
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 44:
		if r.ApiVersion >= 1 {
			return 1
		} else {
			return 0
		}
	case 45:
		return 1
	case 46:
		return 1
	case 47:
		return 0
	default:
		// throw new UnsupportedVersionException("Unsupported API key " + apiKey);
		return -1
	}
}
