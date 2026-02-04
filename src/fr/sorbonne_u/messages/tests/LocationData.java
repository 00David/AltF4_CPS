package fr.sorbonne_u.messages.tests;

import java.io.Serializable;

public class LocationData implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private String longitude;
	private String latitude;
	
	public LocationData(String longitude,String latitude) {
		this.longitude = longitude;
		this.latitude = latitude;
	}
	
	public String getLongitude() {
		return this.longitude;
	}
	
	public String getLatitude() {
		return this.latitude;
	}
}
