package com.graphconcern.common.model;

import java.util.UUID;

import com.graphconcern.resources.exception.InvalidURIDException;

public class URID {

	private static final String DATA_PREFIX = "/resources/data/";

	private String urid;
	private String domain;
	private Types type;
	private UUID uuid;

	public URID(final String urid) throws InvalidURIDException {
		if (null == urid) {
			return;
		}
		this.urid = urid;
		String[] sections = urid.split("/resources/data/");
		if (sections.length != 2) {
			throw new InvalidURIDException(urid);
		}
		this.domain = sections[0];
		sections = sections[1].split("/");
		if (sections.length != 2) {
			throw new InvalidURIDException(urid);
		}
		this.type = Types.getTypeByName(sections[0]);
		this.uuid = UUID.fromString(sections[1]);
	}

	public URID(final String domain, final UUID uuid, final Types type) {
		if (null == domain || null == uuid) {
			return;
		}
		StringBuilder buffer = new StringBuilder();
		buffer.append(domain);
		buffer.append(DATA_PREFIX).append(type.getTypeName()).append('/')
				.append(uuid.toString());
		this.urid = buffer.toString();
		this.domain = domain;
		this.uuid = uuid;
		this.type = type;
	}

	public String getUrid() {
		return urid;
	}

	public void setUrid(String urid) {
		this.urid = urid;
	}

	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public Types getType() {
		return type;
	}

	public void setType(Types type) {
		this.type = type;
	}

	public UUID getUUID() {
		return uuid;
	}

	public void setUUID(UUID uuid) {
		this.uuid = uuid;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((urid == null) ? 0 : urid.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		URID other = (URID) obj;
		if (urid == null) {
			if (other.urid != null)
				return false;
		} else if (!urid.equals(other.urid))
			return false;
		return true;
	}

	
}
