package fr.sorbonne_u.meteo.data;

import fr.sorbonne_u.cps.meteo.interfaces.PositionI;
import fr.sorbonne_u.cps.meteo.interfaces.RegionI;

public class RegionData implements RegionI {

	private double xMin;
	private double xMax;
	private double yMin;
	private double yMax;
	
	@Override
	public boolean in(PositionI p) {
		PositionData pos = (PositionData) p;
		
		if(pos.getPosX() <= this.xMax && pos.getPosX() >= this.xMin && pos.getPosY() <= this.yMax
				&& pos.getPosY() >= this.yMin) {
			return true;
		}
		
		
		return false;
	
	}

}
