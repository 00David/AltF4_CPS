package fr.sorbonne_u.meteo.data;

import fr.sorbonne_u.cps.meteo.interfaces.PositionI;
import fr.sorbonne_u.cps.meteo.interfaces.WindDataI;

/**
 * @author Kevin TANG
 * @author David VADIMON
 */
public class WindData implements WindDataI {

	private static final long serialVersionUID = 8536953812168770777L;

	private PositionData position;
	
	private double windX;
	private double windY;
	
	public WindData(PositionData position, double windX, double windY) {
		this.position = position;
		this.windX = windX;
		this.windY = windY;
	}
	
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
	
	@Override
	public String toString() {
		String res = "WindData\n";
		res += "Position : "+position+"\n";
		res += "Wind vector : X="+windX+" Y="+windY;
		return res;
	}

}
