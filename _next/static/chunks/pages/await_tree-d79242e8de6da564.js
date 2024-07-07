(self.webpackChunk_N_E=self.webpackChunk_N_E||[]).push([[980],{83234:function(e,r,n){"use strict";n.d(r,{NI:function(){return _},Yp:function(){return x},lX:function(){return b}});var t=n(67294),l=n(28387),a=n(76734),i=n(32067),o=n(54520),c=n(52494),s=(...e)=>e.filter(Boolean).join(" "),d=e=>e?"":void 0,u=e=>!!e||void 0;function m(...e){return function(r){e.some(e=>(null==e||e(r),null==r?void 0:r.defaultPrevented))}}var[p,f]=(0,l.k)({name:"FormControlStylesContext",errorMessage:"useFormControlStyles returned is 'undefined'. Seems you forgot to wrap the components in \"<FormControl />\" "}),[h,v]=(0,l.k)({strict:!1,name:"FormControlContext"}),_=(0,i.Gp)(function(e,r){let n=(0,i.jC)("Form",e),{getRootProps:l,htmlProps:c,...u}=function(e){let{id:r,isRequired:n,isInvalid:l,isDisabled:i,isReadOnly:o,...c}=e,s=(0,t.useId)(),u=r||`field-${s}`,m=`${u}-label`,p=`${u}-feedback`,f=`${u}-helptext`,[h,v]=(0,t.useState)(!1),[_,x]=(0,t.useState)(!1),[E,g]=(0,t.useState)(!1),b=(0,t.useCallback)((e={},r=null)=>({id:f,...e,ref:(0,a.lq)(r,e=>{e&&x(!0)})}),[f]),k=(0,t.useCallback)((e={},r=null)=>({...e,ref:r,"data-focus":d(E),"data-disabled":d(i),"data-invalid":d(l),"data-readonly":d(o),id:e.id??m,htmlFor:e.htmlFor??u}),[u,i,E,l,o,m]),y=(0,t.useCallback)((e={},r=null)=>({id:p,...e,ref:(0,a.lq)(r,e=>{e&&v(!0)}),"aria-live":"polite"}),[p]),C=(0,t.useCallback)((e={},r=null)=>({...e,...c,ref:r,role:"group"}),[c]);return{isRequired:!!n,isInvalid:!!l,isReadOnly:!!o,isDisabled:!!i,isFocused:!!E,onFocus:()=>g(!0),onBlur:()=>g(!1),hasFeedbackText:h,setHasFeedbackText:v,hasHelpText:_,setHasHelpText:x,id:u,labelId:m,feedbackId:p,helpTextId:f,htmlProps:c,getHelpTextProps:b,getErrorMessageProps:y,getRootProps:C,getLabelProps:k,getRequiredIndicatorProps:(0,t.useCallback)((e={},r=null)=>({...e,ref:r,role:"presentation","aria-hidden":!0,children:e.children||"*"}),[])}}((0,o.Lr)(e)),m=s("chakra-form-control",e.className);return t.createElement(h,{value:u},t.createElement(p,{value:n},t.createElement(i.m$.div,{...l({},r),className:m,__css:n.container})))});function x(e){let{isDisabled:r,isInvalid:n,isReadOnly:t,isRequired:l,...a}=function(e){let r=v(),{id:n,disabled:t,readOnly:l,required:a,isRequired:i,isInvalid:o,isReadOnly:c,isDisabled:s,onFocus:d,onBlur:u,...p}=e,f=e["aria-describedby"]?[e["aria-describedby"]]:[];return(null==r?void 0:r.hasFeedbackText)&&(null==r?void 0:r.isInvalid)&&f.push(r.feedbackId),(null==r?void 0:r.hasHelpText)&&f.push(r.helpTextId),{...p,"aria-describedby":f.join(" ")||void 0,id:n??(null==r?void 0:r.id),isDisabled:t??s??(null==r?void 0:r.isDisabled),isReadOnly:l??c??(null==r?void 0:r.isReadOnly),isRequired:a??i??(null==r?void 0:r.isRequired),isInvalid:o??(null==r?void 0:r.isInvalid),onFocus:m(null==r?void 0:r.onFocus,d),onBlur:m(null==r?void 0:r.onBlur,u)}}(e);return{...a,disabled:r,readOnly:t,required:l,"aria-invalid":u(n),"aria-required":u(l),"aria-readonly":u(t)}}_.displayName="FormControl",(0,i.Gp)(function(e,r){let n=v(),l=f(),a=s("chakra-form__helper-text",e.className);return t.createElement(i.m$.div,{...null==n?void 0:n.getHelpTextProps(e,r),__css:l.helperText,className:a})}).displayName="FormHelperText";var[E,g]=(0,l.k)({name:"FormErrorStylesContext",errorMessage:"useFormErrorStyles returned is 'undefined'. Seems you forgot to wrap the components in \"<FormError />\" "});(0,i.Gp)((e,r)=>{let n=(0,i.jC)("FormError",e),l=(0,o.Lr)(e),a=v();return(null==a?void 0:a.isInvalid)?t.createElement(E,{value:n},t.createElement(i.m$.div,{...null==a?void 0:a.getErrorMessageProps(l,r),className:s("chakra-form__error-message",e.className),__css:{display:"flex",alignItems:"center",...n.text}})):null}).displayName="FormErrorMessage",(0,i.Gp)((e,r)=>{let n=g(),l=v();if(!(null==l?void 0:l.isInvalid))return null;let a=s("chakra-form__error-icon",e.className);return t.createElement(c.JO,{ref:r,"aria-hidden":!0,...e,__css:n.icon,className:a},t.createElement("path",{fill:"currentColor",d:"M11.983,0a12.206,12.206,0,0,0-8.51,3.653A11.8,11.8,0,0,0,0,12.207,11.779,11.779,0,0,0,11.8,24h.214A12.111,12.111,0,0,0,24,11.791h0A11.766,11.766,0,0,0,11.983,0ZM10.5,16.542a1.476,1.476,0,0,1,1.449-1.53h.027a1.527,1.527,0,0,1,1.523,1.47,1.475,1.475,0,0,1-1.449,1.53h-.027A1.529,1.529,0,0,1,10.5,16.542ZM11,12.5v-6a1,1,0,0,1,2,0v6a1,1,0,1,1-2,0Z"}))}).displayName="FormErrorIcon";var b=(0,i.Gp)(function(e,r){let n=(0,i.mq)("FormLabel",e),l=(0,o.Lr)(e),{className:a,children:c,requiredIndicator:d=t.createElement(k,null),optionalIndicator:u=null,...m}=l,p=v(),f=(null==p?void 0:p.getLabelProps(m,r))??{ref:r,...m};return t.createElement(i.m$.label,{...f,className:s("chakra-form__label",l.className),__css:{display:"block",textAlign:"start",...n}},c,(null==p?void 0:p.isRequired)?d:u)});b.displayName="FormLabel";var k=(0,i.Gp)(function(e,r){let n=v(),l=f();if(!(null==n?void 0:n.isRequired))return null;let a=s("chakra-form__required-indicator",e.className);return t.createElement(i.m$.span,{...null==n?void 0:n.getRequiredIndicatorProps(e,r),__css:l.requiredIndicator,className:a})});k.displayName="RequiredIndicator"},57026:function(e,r,n){"use strict";n.d(r,{Ph:function(){return d}});var t=n(67294),l=n(83234),a=n(32067),i=n(54520),o=(...e)=>e.filter(Boolean).join(" "),c=e=>e?"":void 0,s=(0,a.Gp)(function(e,r){let{children:n,placeholder:l,className:i,...c}=e;return t.createElement(a.m$.select,{...c,ref:r,className:o("chakra-select",i)},l&&t.createElement("option",{value:""},l),n)});s.displayName="SelectField";var d=(0,a.Gp)((e,r)=>{var n;let o=(0,a.jC)("Select",e),{rootProps:d,placeholder:u,icon:m,color:f,height:h,h:v,minH:_,minHeight:x,iconColor:E,iconSize:g,...b}=(0,i.Lr)(e),[k,y]=function(e,r){let n={},t={};for(let[l,a]of Object.entries(e))r.includes(l)?n[l]=a:t[l]=a;return[n,t]}(b,i.oE),C=(0,l.Yp)(y),N={paddingEnd:"2rem",...o.field,_focus:{zIndex:"unset",...null==(n=o.field)?void 0:n._focus}};return t.createElement(a.m$.div,{className:"chakra-select__wrapper",__css:{width:"100%",height:"fit-content",position:"relative",color:f},...k,...d},t.createElement(s,{ref:r,height:v??h,minH:_??x,placeholder:u,...C,__css:N},e.children),t.createElement(p,{"data-disabled":c(C.disabled),...(E||f)&&{color:E||f},__css:o.icon,...g&&{fontSize:g}},m))});d.displayName="Select";var u=e=>t.createElement("svg",{viewBox:"0 0 24 24",...e},t.createElement("path",{fill:"currentColor",d:"M16.59 8.59L12 13.17 7.41 8.59 6 10l6 6 6-6z"})),m=(0,a.m$)("div",{baseStyle:{position:"absolute",display:"inline-flex",alignItems:"center",justifyContent:"center",pointerEvents:"none",top:"50%",transform:"translateY(-50%)"}}),p=e=>{let{children:r=t.createElement(u,null),...n}=e,l=(0,t.cloneElement)(r,{role:"presentation",className:"chakra-select__icon",focusable:!1,"aria-hidden":!0,style:{width:"1em",height:"1em",color:"currentColor"}});return t.createElement(m,{...n,className:"chakra-select__icon-wrapper"},(0,t.isValidElement)(r)?l:null)};p.displayName="SelectIcon"},92299:function(e,r,n){(window.__NEXT_P=window.__NEXT_P||[]).push(["/await_tree",function(){return n(4225)}])},4225:function(e,r,n){"use strict";n.r(r),n.d(r,{default:function(){return g}});var t=n(85893),l=n(40639),a=n(83234),i=n(57026),o=n(47741),c=n(63764),s=n(96486),d=n.n(s),u=n(9008),m=n.n(u),p=n(67294),f=n(65098),h=n(6448),v=n(43231),_=n(3526),x=n(44162),E=n(86357);function g(){let{response:e}=(0,x.Z)(_.OL),[r,n]=(0,p.useState)(),[s,u]=(0,p.useState)("");(0,p.useEffect)(()=>{e&&!r&&n("")},[e,r]);let g=async()=>{let e,n;if(void 0!==r){e=""===r?"Await-Tree Dump of All Compute Nodes:":"Await-Tree Dump of Compute Node ".concat(r,":"),u("Loading...");try{let t=E.Kv.fromJSON(await v.ZP.get("/monitor/await_tree/".concat(r))),l=d()(t.actorTraces).entries().map(e=>{let[r,n]=e;return"[Actor ".concat(r,"]\n").concat(n)}).join("\n"),a=d()(t.rpcTraces).entries().map(e=>{let[r,n]=e;return"[RPC ".concat(r,"]\n").concat(n)}).join("\n"),i=d()(t.compactionTaskTraces).entries().map(e=>{let[r,n]=e;return"[Compaction ".concat(r,"]\n").concat(n)}).join("\n"),o=d()(t.inflightBarrierTraces).entries().map(e=>{let[r,n]=e;return"[Barrier ".concat(r,"]\n").concat(n)}).join("\n"),c=d()(t.barrierWorkerState).entries().map(e=>{let[r,n]=e;return"[BarrierWorkerState (Worker ".concat(r,")]\n").concat(n)}).join("\n"),s=d()(t.jvmStackTraces).entries().map(e=>{let[r,n]=e;return"[JVM (Worker ".concat(r,")]\n").concat(n)}).join("\n");n="".concat(e,"\n\n").concat(l,"\n").concat(a,"\n").concat(i,"\n").concat(o,"\n").concat(c,"\n\n").concat(s)}catch(r){n="".concat(e,"\n\nERROR: ").concat(r.message,"\n").concat(r.cause)}u(n)}},b=(0,t.jsxs)(l.kC,{p:3,height:"calc(100vh - 20px)",flexDirection:"column",children:[(0,t.jsx)(h.Z,{children:"Await Tree Dump"}),(0,t.jsxs)(l.kC,{flexDirection:"row",height:"full",width:"full",children:[(0,t.jsx)(l.gC,{mr:3,spacing:3,alignItems:"flex-start",width:200,height:"full",children:(0,t.jsxs)(a.NI,{children:[(0,t.jsx)(a.lX,{children:"Compute Nodes"}),(0,t.jsxs)(l.gC,{children:[(0,t.jsxs)(i.Ph,{onChange:e=>n(e.target.value),children:[e&&(0,t.jsx)("option",{value:"",children:"All"},""),e&&e.map(e=>{var r,n;return(0,t.jsxs)("option",{value:e.id,children:["(",e.id,") ",null===(r=e.host)||void 0===r?void 0:r.host,":",null===(n=e.host)||void 0===n?void 0:n.port]},e.id)})]}),(0,t.jsx)(o.zx,{onClick:e=>g(),width:"full",children:"Dump"})]})]})}),(0,t.jsx)(l.xu,{flex:1,height:"full",ml:3,overflowX:"scroll",overflowY:"scroll",children:void 0===s?(0,t.jsx)(f.Z,{}):(0,t.jsx)(c.ZP,{language:"sql",options:{fontSize:13,readOnly:!0,renderWhitespace:"boundary",wordWrap:"on"},defaultValue:'Select a compute node and click "Dump"...',value:s})})]})]});return(0,t.jsxs)(p.Fragment,{children:[(0,t.jsx)(m(),{children:(0,t.jsx)("title",{children:"Await Tree Dump"})}),b]})}},9008:function(e,r,n){e.exports=n(6665)}},function(e){e.O(0,[662,958,425,888,774,179],function(){return e(e.s=92299)}),_N_E=e.O()}]);