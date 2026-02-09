package fr.sorbonne_u.meteo.data;

import fr.sorbonne_u.cps.meteo.interfaces.PositionI;
import fr.sorbonne_u.cps.meteo.interfaces.WindDataI;

public class WindData implements WindDataI {

	private static final long serialVersionUID = 1L;
	private PositionData position;
	

	private double windX;
	private double windY;
	
	
	
	
	@Override
	public PositionI getPosition() {
		return this.position;
	}

	@Override
	public double xComponent() {
		return windX;
	}

	@Override
	public double yComponent() {
		return windY;
	}

	@Override
	public double force() {
		return Math.sqrt((windX-position.getPosX())+(windY-position.getPosY()));
	}

}
