package fr.sorbonne_u.meteo.data;

import fr.sorbonne_u.cps.meteo.interfaces.PositionI;

/**
 * @author Kevin TANG
 * @author David VADIMON
 */
public class PositionData implements PositionI {
	
	private static final long serialVersionUID = -4464305380366957098L;
	private double posX;
	private double posY;
	
	public PositionData(double posX,double posY) {
		this.posX = posX;
		this.posY = posY;
	}
	
	
	public double getPosX() {
		return this.posX;
	}
	
	public double getPosY() {
		return this.posY;
	}
	
	@Override
	public boolean equals(PositionI p) {
		PositionData pos = (PositionData) p;
		return (this.posX == pos.posX) && (this.posY == pos.posY);
	}
	
	@Override
	public String toString() {
		return "("+posX+","+posY+")";
	}
}