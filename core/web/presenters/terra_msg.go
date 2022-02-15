package presenters

// TerraMsgResource repesents a Terra message JSONAPI resource.
type TerraMsgResource struct {
	JAID
	ChainID    string
	ContractID string
	State      string
	Raw        []byte // serialized msg
	TxHash     *string
}

// GetName implements the api2go EntityNamer interface
func (TerraMsgResource) GetName() string {
	return "terra_messages"
}

func NewTerraMsgResource(id int64, chainID string, contractID string, raw []byte) TerraMsgResource {
	return TerraMsgResource{
		JAID:       NewJAIDInt64(id),
		ChainID:    chainID,
		ContractID: contractID,
		Raw:        raw,
	}
}
