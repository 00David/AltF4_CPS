package fr.sorbonne_u.meteo.data;

import fr.sorbonne_u.cps.meteo.interfaces.PositionI;

public class PositionData implements PositionI {

	private static final long serialVersionUID = 1L;
	
	private double posX;
	private double posY;
	
	public PositionData(double posX,double posY) {
		this.posX = posX;
		this.posY = posY;
	}
	

	@Override
	public boolean equals(PositionI p) {
		PositionData pos = (PositionData) p;
		
		return (this.posX == pos.posX) && (this.posY == pos.posY);
		
	}
	
	
	public double getPosX() {
		return this.posX;
	}
	
	public double getPosY() {
		return this.posY;
	}
	
	

}
