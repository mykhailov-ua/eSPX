(function () {
	const ICON_PX = 16;

	const paths = {
		check(ctx) {
			ctx.beginPath();
			ctx.moveTo(3, 8.5);
			ctx.lineTo(7, 12.5);
			ctx.lineTo(13, 4.5);
			ctx.stroke();
		},
		x(ctx) {
			ctx.beginPath();
			ctx.moveTo(4, 4);
			ctx.lineTo(12, 12);
			ctx.moveTo(12, 4);
			ctx.lineTo(4, 12);
			ctx.stroke();
		},
		info(ctx) {
			const color = ctx.strokeStyle;
			ctx.beginPath();
			ctx.arc(8, 8, 6.5, 0, Math.PI * 2);
			ctx.stroke();
			ctx.beginPath();
			ctx.moveTo(8, 7.5);
			ctx.lineTo(8, 11.5);
			ctx.stroke();
			ctx.beginPath();
			ctx.arc(8, 5.25, 1.35, 0, Math.PI * 2);
			ctx.fillStyle = color;
			ctx.fill();
		},
		key(ctx) {
			ctx.beginPath();
			ctx.arc(6, 6, 3.25, 0, Math.PI * 2);
			ctx.stroke();
			ctx.beginPath();
			ctx.moveTo(8.5, 8.5);
			ctx.lineTo(13, 13);
			ctx.stroke();
			ctx.beginPath();
			ctx.moveTo(11, 11);
			ctx.lineTo(13, 11);
			ctx.moveTo(12, 10);
			ctx.lineTo(12, 12);
			ctx.stroke();
		},
		trash(ctx) {
			ctx.beginPath();
			ctx.moveTo(4, 5.5);
			ctx.lineTo(12, 5.5);
			ctx.moveTo(6, 5.5);
			ctx.lineTo(6.5, 3.5);
			ctx.lineTo(9.5, 3.5);
			ctx.lineTo(10, 5.5);
			ctx.stroke();
			ctx.strokeRect(5, 5.5, 6, 8);
			ctx.beginPath();
			ctx.moveTo(7, 8);
			ctx.lineTo(7, 11);
			ctx.moveTo(9, 8);
			ctx.lineTo(9, 11);
			ctx.stroke();
		},
	};

	function strokeColor() {
		const style = getComputedStyle(document.documentElement);
		return style.getPropertyValue('--text-muted').trim() || '#888888';
	}

	function paintIcon(root) {
		const name = root.getAttribute('data-icon');
		const draw = paths[name];
		if (!draw) return;

		const canvas = root.querySelector('canvas');
		if (!canvas) return;

		const dpr = window.devicePixelRatio || 1;
		canvas.width = ICON_PX * dpr;
		canvas.height = ICON_PX * dpr;
		canvas.style.width = ICON_PX + 'px';
		canvas.style.height = ICON_PX + 'px';

		const ctx = canvas.getContext('2d');
		if (!ctx) return;

		ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
		ctx.clearRect(0, 0, ICON_PX, ICON_PX);
		ctx.strokeStyle = strokeColor();
		ctx.lineWidth = 1.5;
		ctx.lineCap = 'butt';
		ctx.lineJoin = 'miter';
		draw(ctx);
	}

	function initIcons(scope) {
		const root = scope && scope.querySelectorAll ? scope : document;
		root.querySelectorAll('[data-icon]').forEach(paintIcon);
	}

	window.initIcons = initIcons;
	document.addEventListener('DOMContentLoaded', () => initIcons(document));
	document.body.addEventListener('htmx:afterSwap', (event) => initIcons(event.detail.target));
})();
